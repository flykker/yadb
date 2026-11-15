//! Path-style AWS S3 API. One listener is bound to a declarative project/ObjectStore.
use super::*;
use crate::*;
use axum::{extract::Request, http::Method};
use base64::Engine;

#[derive(Clone)]
pub struct S3State {
    pub app: AppState,
    pub credentials: Credentials,
    pub project: String,
    pub store: String,
}
fn xml(code: StatusCode, body: String) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "application/xml")],
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>{body}"),
    )
        .into_response()
}
fn esc(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn date(seconds: u64) -> String {
    chrono::DateTime::from_timestamp(seconds.min(i64::MAX as u64) as i64, 0)
        .unwrap_or_default()
        .format("%Y-%m-%dT%H:%M:%S.000Z")
        .to_string()
}
fn err(status: StatusCode, code: &str, msg: &str) -> Response {
    xml(
        status,
        format!(
            "<Error><Code>{code}</Code><Message>{}</Message></Error>",
            esc(msg)
        ),
    )
}

async fn discard_read_lease(app: &AppState, lease: Option<yadb_metakv::GcLease>) {
    if let Some(lease) = lease {
        let app = app.clone();
        let _ = tokio::task::spawn_blocking(move || app.end_read_lease(&lease)).await;
    }
}

/// The `If-Match`/`If-None-Match` decision of the native listener, applied to
/// the ETag a read actually produced.
///
/// A proxied read evaluates it against the leader's response, not against the
/// follower's local manifest: the object can be overwritten between the two, and
/// a conditional request must never return bytes that violate its condition.
fn evaluate_conditions(headers: &HeaderMap, etag: &str) -> Option<Response> {
    if headers
        .get(header::IF_MATCH)
        .is_some_and(|v| v.as_bytes() != etag.as_bytes())
    {
        return Some(err(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "ETag mismatch",
        ));
    }
    if headers
        .get(header::IF_NONE_MATCH)
        .is_some_and(|v| v.as_bytes() == etag.as_bytes())
    {
        return Some(StatusCode::NOT_MODIFIED.into_response());
    }
    None
}

/// The range and size rules the native listener applies to a GET.
///
/// A read that is forwarded must satisfy them *before* it leaves the node: the
/// internal gateway has no 16 MiB limit and answers its own JSON error
/// vocabulary, so a request the local handler would refuse has to be refused
/// here with the local status and headers (`416 InvalidRange` with
/// `Content-Range: bytes */size`, or `400 InvalidRequest`).
fn preflight_range(headers: &HeaderMap, size: u64) -> Result<Option<(u64, u64)>, Response> {
    let range = match headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        Some(value) => match parse_range_header(value, size) {
            Some(range) => Some(range),
            None => {
                let mut response = err(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "InvalidRange",
                    "range not satisfiable",
                );
                response.headers_mut().insert(
                    header::CONTENT_RANGE,
                    format!("bytes */{size}").parse().unwrap(),
                );
                return Err(response);
            }
        },
        None => None,
    };
    if size > 16 * 1024 * 1024 && range.is_none() {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "use Range for objects over 16 MiB",
        ));
    }
    if let Some((start, end)) = range {
        if end - start + 1 > 16 * 1024 * 1024 {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "range exceeds 16 MiB",
            ));
        }
    }
    Ok(range)
}
fn query(raw: &str) -> io::Result<Vec<(String, String)>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split('&')
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            Ok((decode(k)?, decode(v)?))
        })
        .collect()
}

fn authenticate(
    headers: &HeaderMap,
    method: &Method,
    path: &str,
    q: &[(String, String)],
    body: &[u8],
    credentials: &Credentials,
) -> io::Result<String> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| io::Error::other("SigV4 Authorization required"))?;
    let auth = auth
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(|| io::Error::other("unsupported signing algorithm"))?;
    let fields: BTreeMap<_, _> = auth
        .split(',')
        .filter_map(|v| v.trim().split_once('='))
        .collect();
    let scope = fields
        .get("Credential")
        .ok_or_else(|| io::Error::other("missing credential"))?
        .split('/')
        .collect::<Vec<_>>();
    if scope.len() != 5 || scope[3] != "s3" || scope[4] != "aws4_request" {
        return Err(io::Error::other("invalid credential scope"));
    }
    let cred = credentials
        .users
        .get(scope[0])
        .ok_or_else(|| io::Error::other("unknown access key"))?;
    let date = headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| io::Error::other("missing date"))?;
    let timestamp = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ")
        .map_err(io::Error::other)?
        .and_utc()
        .timestamp();
    if (chrono::Utc::now().timestamp() - timestamp).abs() > 900 || !date.starts_with(scope[1]) {
        return Err(io::Error::other("request expired"));
    }
    if headers.contains_key("x-amz-security-token") {
        return Err(io::Error::other("temporary credentials unsupported"));
    }
    let signed = *fields
        .get("SignedHeaders")
        .ok_or_else(|| io::Error::other("missing SignedHeaders"))?;
    let names: Vec<_> = signed.split(';').collect();
    if !names.contains(&"host")
        || !names.contains(&"x-amz-date")
        || names.windows(2).any(|v| v[0] >= v[1])
    {
        return Err(io::Error::other("invalid signed headers"));
    }
    let mut canonical_headers = String::new();
    for name in names {
        if name != name.to_ascii_lowercase() {
            return Err(io::Error::other("invalid header name"));
        }
        let vals = headers
            .get_all(name)
            .iter()
            .map(|v| {
                v.to_str()
                    .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
                    .map_err(io::Error::other)
            })
            .collect::<io::Result<Vec<_>>>()?;
        if vals.is_empty() {
            return Err(io::Error::other("missing signed header"));
        }
        canonical_headers.push_str(&format!("{name}:{}\n", vals.join(",")));
    }
    let payload = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| io::Error::other("payload SHA256 required"))?;
    if payload != "UNSIGNED-PAYLOAD" && !equal(payload.as_bytes(), sha(body).as_bytes()) {
        return Err(io::Error::other(
            "payload digest mismatch or unsupported streaming signature",
        ));
    }
    let mut cq = q.iter().map(|(k, v)| (url(k), url(v))).collect::<Vec<_>>();
    cq.sort();
    let cq = cq
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_path = path
        .split('/')
        .map(|s| decode(s).map(|s| url(&s)))
        .collect::<io::Result<Vec<_>>>()?
        .join("/");
    let canonical =
        format!("{method}\n{canonical_path}\n{cq}\n{canonical_headers}\n{signed}\n{payload}");
    let scope_str = scope[1..].join("/");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope_str}\n{}",
        sha(canonical.as_bytes())
    );
    let k = hmac(
        format!("AWS4{}", cred.secret).as_bytes(),
        scope[1].as_bytes(),
    );
    let k = hmac(&k, scope[2].as_bytes());
    let k = hmac(&k, b"s3");
    let k = hmac(&k, b"aws4_request");
    let signature = fields
        .get("Signature")
        .and_then(|v| unhex(v).ok())
        .ok_or_else(|| io::Error::other("invalid signature"))?;
    if !equal(&signature, &hmac(&k, to_sign.as_bytes())) {
        return Err(io::Error::other("signature mismatch"));
    }
    Ok(cred.subject.clone())
}

pub async fn handle(State(st): State<S3State>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let method = parts.method;
    let headers = parts.headers;
    let uri = parts.uri;
    // Explicitly bounded buffered uploads. Never advertise unbounded streaming.
    let body = match axum::body::to_bytes(body, 16 * 1024 * 1024).await {
        Ok(v) => v,
        Err(_) => {
            return err(
                StatusCode::PAYLOAD_TOO_LARGE,
                "EntityTooLarge",
                "maximum request body is 16 MiB",
            )
        }
    };
    let q = match query(uri.query().unwrap_or("")) {
        Ok(v) => v,
        Err(_) => {
            return err(
                StatusCode::BAD_REQUEST,
                "InvalidArgument",
                "invalid query string",
            )
        }
    };
    let subject = match authenticate(&headers, &method, uri.path(), &q, &body, &st.credentials) {
        Ok(v) => v,
        Err(e) => {
            return err(
                StatusCode::FORBIDDEN,
                "SignatureDoesNotMatch",
                &e.to_string(),
            )
        }
    };
    let path = match decode(uri.path()) {
        Ok(v) => v,
        Err(_) => return err(StatusCode::BAD_REQUEST, "InvalidURI", "invalid path"),
    };
    let path = path.strip_prefix('/').unwrap_or(&path);
    let (bucket, key) = path.split_once('/').unwrap_or((path, ""));
    let params: BTreeMap<_, _> = q.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    if q.iter().any(|(k, _)| {
        ![
            "list-type",
            "prefix",
            "delimiter",
            "max-keys",
            "continuation-token",
            "start-after",
            "marker",
            "encoding-type",
            "location",
            "x-id",
        ]
        .contains(&k.as_str())
    }) {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "unsupported S3 subresource or parameter",
        );
    }
    let write = method == Method::PUT || method == Method::DELETE || method == Method::POST;
    let (pid, pool, buckets) = {
        let cp = match st.app.control_for_authorization_async().await {
            Ok(cp) => cp,
            Err(_) => {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ServiceUnavailable",
                    "control plane stale or unavailable",
                )
            }
        };
        let pid = match cp.authorize(
            &st.project,
            &subject,
            if write {
                Role::ReadWrite
            } else {
                Role::ReadOnly
            },
        ) {
            Ok(v) => v,
            Err(_) => {
                return err(
                    StatusCode::FORBIDDEN,
                    "AccessDenied",
                    "project access denied",
                )
            }
        };
        let Some(spec) = cp
            .object_stores
            .get(&format!("{}/{}", st.project, st.store))
        else {
            return err(
                StatusCode::NOT_FOUND,
                "NoSuchBucket",
                "ObjectStore not declared",
            );
        };
        (pid, spec.pool.clone(), spec.buckets.clone())
    };
    if bucket.is_empty() && method == Method::GET {
        return xml(StatusCode::OK,format!("<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>yadb</ID><DisplayName>yadb</DisplayName></Owner><Buckets>{}</Buckets></ListAllMyBucketsResult>",buckets.iter().map(|b|format!("<Bucket><Name>{}</Name><CreationDate>2026-01-01T00:00:00.000Z</CreationDate></Bucket>",esc(b))).collect::<String>()));
    }
    if bucket.is_empty() || (!buckets.is_empty() && !buckets.iter().any(|b| b == bucket)) {
        return err(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "bucket is not declared",
        );
    }
    if params.contains_key("uploads")
        || params.contains_key("uploadId")
        || params.contains_key("delete")
        || headers.contains_key("x-amz-copy-source")
    {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "multipart uploads, bulk delete and server-side copy are not supported yet",
        );
    }
    if key.is_empty() {
        if method == Method::HEAD {
            return StatusCode::OK.into_response();
        }
        if method == Method::GET && params.contains_key("location") {
            return xml(StatusCode::OK,"<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">us-east-1</LocationConstraint>".into());
        }
        if method == Method::GET {
            let prefix = params.get("prefix").copied().unwrap_or("");
            let delimiter = params.get("delimiter").copied().unwrap_or("");
            let max = match params
                .get("max-keys")
                .map(|v| v.parse::<usize>())
                .transpose()
            {
                Ok(v) => v.unwrap_or(1000).min(1000),
                Err(_) => {
                    return err(
                        StatusCode::BAD_REQUEST,
                        "InvalidArgument",
                        "invalid max-keys",
                    )
                }
            };
            let after = if let Some(t) = params.get("continuation-token") {
                match unhex(t).and_then(|v| String::from_utf8(v).map_err(io::Error::other)) {
                    Ok(v) => v,
                    Err(_) => {
                        return err(
                            StatusCode::BAD_REQUEST,
                            "InvalidArgument",
                            "invalid continuation token",
                        )
                    }
                }
            } else {
                params
                    .get("start-after")
                    .or(params.get("marker"))
                    .copied()
                    .unwrap_or("")
                    .to_string()
            };
            let engine = st.app.s3.clone();
            let store = st.store.clone();
            let b = bucket.to_string();
            let pre = prefix.to_string();
            let objs = match tokio::task::spawn_blocking(move || {
                engine.list_objects(pid, &store, &b, &pre)
            })
            .await
            {
                Ok(Ok(v)) => v,
                _ => {
                    return err(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "ServiceUnavailable",
                        "listing failed",
                    )
                }
            };
            let mut entries = BTreeMap::<String, Option<yadb_s3::ObjectSummary>>::new();
            for obj in objs {
                let common = if delimiter.is_empty() {
                    None
                } else {
                    obj.key[prefix.len()..]
                        .find(delimiter)
                        .map(|i| obj.key[..prefix.len() + i + delimiter.len()].to_string())
                };
                let k = common.clone().unwrap_or_else(|| obj.key.clone());
                if k <= after {
                    continue;
                }
                entries.insert(k, if common.is_some() { None } else { Some(obj) });
            }
            let truncated = max > 0 && entries.len() > max;
            let v2 = params.get("list-type") == Some(&"2");
            let selected = entries.into_iter().take(max).collect::<Vec<_>>();
            let encode = params.get("encoding-type") == Some(&"url");
            let name = |s: &str| if encode { url(s) } else { esc(s) };
            let mut out=format!("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{max}</MaxKeys><IsTruncated>{truncated}</IsTruncated>",esc(bucket),name(prefix));
            if encode {
                out.push_str("<EncodingType>url</EncodingType>");
            }
            if v2 {
                out.push_str(&format!("<KeyCount>{}</KeyCount>", selected.len()));
            }
            for (k, obj) in &selected {
                if let Some(obj) = obj {
                    out.push_str(&format!("<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",name(k),date(obj.last_modified),esc(&obj.etag),obj.size));
                } else {
                    out.push_str(&format!(
                        "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                        name(k)
                    ));
                }
            }
            if truncated {
                let last = &selected.last().unwrap().0;
                if v2 {
                    out.push_str(&format!(
                        "<NextContinuationToken>{}</NextContinuationToken>",
                        hex(last.as_bytes())
                    ));
                } else {
                    out.push_str(&format!("<NextMarker>{}</NextMarker>", name(last)));
                }
            }
            out.push_str("</ListBucketResult>");
            return xml(StatusCode::OK, out);
        }
        return err(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "buckets are managed declaratively",
        );
    }
    let mut gateway_headers = HeaderMap::new();
    gateway_headers.insert(
        "x-yadb-subject",
        match subject.parse() {
            Ok(v) => v,
            Err(_) => return err(StatusCode::FORBIDDEN, "AccessDenied", "invalid subject"),
        },
    );
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        gateway_headers.insert(header::CONTENT_TYPE, ct.clone());
    }
    // A keyed mutation is scoped by subject and the gateway handlers require a
    // proven identity. SigV4 already proved it here, so forward the key and
    // vouch for the same subject with the internal signature. This is only done
    // for keyed PUT/DELETE: an unkeyed request keeps its previous headers
    // byte-for-byte.
    if (method == Method::PUT || method == Method::DELETE)
        && headers.contains_key("idempotency-key")
    {
        if let Some(value) = headers.get("idempotency-key") {
            gateway_headers.insert("idempotency-key", value.clone());
        }
        if let Some(secret) = st.app.subject_secret.as_deref() {
            match subject_signature(secret, &subject).parse() {
                Ok(v) => {
                    gateway_headers.insert("x-yadb-subject-signature", v);
                }
                Err(_) => return err(StatusCode::FORBIDDEN, "AccessDenied", "invalid subject"),
            }
        }
    }
    let path = (
        st.project.clone(),
        st.store.clone(),
        bucket.to_string(),
        key.to_string(),
    );
    match method {
        Method::PUT => {
            if headers
                .keys()
                .any(|k| k.as_str().starts_with("x-amz-meta-"))
                || headers.contains_key(header::IF_MATCH)
                || headers.contains_key(header::IF_NONE_MATCH)
            {
                return err(
                    StatusCode::NOT_IMPLEMENTED,
                    "NotImplemented",
                    "conditional PUT and custom metadata are not supported",
                );
            }
            for name in [
                "x-amz-server-side-encryption",
                "x-amz-acl",
                "x-amz-checksum-crc32c",
                "x-amz-checksum-crc64nvme",
                "x-amz-checksum-sha1",
            ] {
                if headers.contains_key(name) {
                    return err(
                        StatusCode::NOT_IMPLEMENTED,
                        "NotImplemented",
                        "requested encryption, ACL or checksum is unsupported",
                    );
                }
            }
            for (name, value) in [
                ("content-md5", super::iscsi::md5(&body).to_vec()),
                (
                    "x-amz-checksum-crc32",
                    crc32fast::hash(&body).to_be_bytes().to_vec(),
                ),
                (
                    "x-amz-checksum-sha256",
                    ring::digest::digest(&ring::digest::SHA256, &body)
                        .as_ref()
                        .to_vec(),
                ),
            ] {
                if let Some(got) = headers.get(name) {
                    let expected = base64::engine::general_purpose::STANDARD.encode(value);
                    if got.as_bytes() != expected.as_bytes() {
                        return err(StatusCode::BAD_REQUEST, "BadDigest", "checksum mismatch");
                    }
                }
            }
            let response =
                handle_s3_put(State(st.app.clone()), gateway_headers, Path(path), body).await;
            if response.status().is_success() {
                let mut r = StatusCode::OK.into_response();
                if let Some(v) = response.headers().get(header::ETAG) {
                    r.headers_mut().insert(header::ETAG, v.clone());
                }
                r
            } else {
                map_error(response).await
            }
        }
        Method::GET | Method::HEAD => {
            // A legacy read is fenced by a reader lease, and only the metadata
            // leader can publish one. A follower therefore forwards the request
            // it has already authenticated (with its `Range` header) to the
            // leader's gateway route instead of serving its own copy unfenced or
            // failing on the lease. On the leader this returns `None` and the
            // read runs locally under the lease taken below. HEAD reads no
            // fragments, so it needs no fence and stays local.
            if method == Method::GET && !st.app.meta.is_openraft() {
                // Validate against local metadata first. The manifest read takes
                // no lease (only the fragment fetch does), and applying the local
                // contract here keeps `404`, `304`, `412`, `416` and the 16 MiB
                // rules identical whether the read is served here or forwarded.
                let engine = st.app.s3.clone();
                let (p, s, b, k) = (pid, st.store.clone(), bucket.to_string(), key.to_string());
                let head =
                    match tokio::task::spawn_blocking(move || engine.head_object(p, &s, &b, &k))
                        .await
                    {
                        Ok(Ok(head)) => head,
                        _ => return err(StatusCode::NOT_FOUND, "NoSuchKey", "object not found"),
                    };
                if let Some(response) = evaluate_conditions(&headers, &head.etag) {
                    return response;
                }
                if let Err(response) = preflight_range(&headers, head.size) {
                    return response;
                }
                let mut forwarded = gateway_headers.clone();
                if let Some(secret) = st.app.subject_secret.as_deref() {
                    match subject_signature(secret, &subject).parse() {
                        Ok(v) => {
                            forwarded.insert("x-yadb-subject-signature", v);
                        }
                        Err(_) => {
                            return err(StatusCode::FORBIDDEN, "AccessDenied", "invalid subject")
                        }
                    }
                }
                if let Some(range) = headers.get(header::RANGE) {
                    forwarded.insert(header::RANGE, range.clone());
                }
                let gateway_path = format!(
                    "/s3/{}/{}/{}/{}",
                    crate::protocols::url(&st.project),
                    crate::protocols::url(&st.store),
                    crate::protocols::url(bucket),
                    key.split('/')
                        .map(crate::protocols::url)
                        .collect::<Vec<_>>()
                        .join("/")
                );
                if let Some(response) = crate::maybe_forward_write(
                    &st.app,
                    &forwarded,
                    Some(&st.project),
                    "GET",
                    gateway_path,
                    "application/octet-stream".into(),
                    axum::body::Bytes::new(),
                )
                .await
                {
                    // A successful read is returned as-is (body, ETag and
                    // Content-Range). A failure is re-framed as an S3 XML error,
                    // because the gateway answers JSON and a native S3 client
                    // parses the wire format.
                    if response.status().is_success() {
                        if let Some(etag) = response
                            .headers()
                            .get(header::ETAG)
                            .and_then(|v| v.to_str().ok())
                        {
                            // Re-check against the version actually read: the
                            // preflight above only saw the follower's manifest.
                            if let Some(conditional) = evaluate_conditions(&headers, etag) {
                                return conditional;
                            }
                        }
                        let mut response = response;
                        // The internal hop's identity is not part of the native
                        // wire format.
                        response.headers_mut().remove("x-yadb-leader-id");
                        return response;
                    }
                    return map_error(response).await;
                }
            }
            let engine = st.app.s3.clone();
            let store = st.store.clone();
            let b = bucket.to_string();
            let k = key.to_string();
            let mut read_lease = if method == Method::GET && !st.app.meta.is_openraft() {
                let app = st.app.clone();
                match tokio::task::spawn_blocking(move || app.begin_read_lease(pid)).await {
                    Ok(Ok(lease)) => Some(lease),
                    Ok(Err(_)) | Err(_) => {
                        return err(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "ServiceUnavailable",
                            "reader lease unavailable",
                        )
                    }
                }
            } else {
                None
            };
            // The legacy lease covers both the manifest read and chunk fetch;
            // OpenRaft does not need this fence because its collection path is
            // not raced by the legacy collector.
            let head =
                match tokio::task::spawn_blocking(move || engine.head_object(pid, &store, &b, &k))
                    .await
                {
                    Ok(Ok(v)) => v,
                    _ => {
                        discard_read_lease(&st.app, read_lease.take()).await;
                        return err(StatusCode::NOT_FOUND, "NoSuchKey", "object not found");
                    }
                };
            if let Some(response) = evaluate_conditions(&headers, &head.etag) {
                discard_read_lease(&st.app, read_lease.take()).await;
                return response;
            }
            let modified = crate::http_date(head.last_modified);
            if method == Method::HEAD {
                discard_read_lease(&st.app, read_lease.take()).await;
                return Response::builder()
                    .header(header::CONTENT_LENGTH, head.size)
                    .header(header::CONTENT_TYPE, head.content_type)
                    .header(header::ETAG, head.etag)
                    .header(header::LAST_MODIFIED, modified)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
            let range = match preflight_range(&headers, head.size) {
                Ok(range) => range,
                Err(response) => {
                    discard_read_lease(&st.app, read_lease.take()).await;
                    return response;
                }
            };
            let engine = st.app.s3.clone();
            let snapshot = head.clone();
            let read = tokio::task::spawn_blocking(move || {
                if snapshot.size == 0 {
                    Ok(Vec::new())
                } else {
                    let (start, end) = range.unwrap_or((0, snapshot.size - 1));
                    engine.read_manifest_range(&snapshot, start, end)
                }
            })
            .await;
            let bytes = match read {
                Ok(Ok(bytes)) => bytes,
                _ => {
                    discard_read_lease(&st.app, read_lease.take()).await;
                    return err(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "ServiceUnavailable",
                        "object read failed",
                    );
                }
            };
            if let Some(lease) = read_lease.take() {
                let app = st.app.clone();
                match tokio::task::spawn_blocking(move || app.end_read_lease(&lease)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => {
                        return err(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "ServiceUnavailable",
                            "reader lease lost during object read",
                        )
                    }
                }
            }
            let mut response = Response::builder()
                .status(if range.is_some() {
                    StatusCode::PARTIAL_CONTENT
                } else {
                    StatusCode::OK
                })
                .header(header::CONTENT_TYPE, head.content_type)
                .header(header::ETAG, head.etag)
                .header(header::LAST_MODIFIED, modified)
                .header(header::ACCEPT_RANGES, "bytes");
            if let Some((a, b)) = range {
                response = response.header(
                    header::CONTENT_RANGE,
                    format!("bytes {a}-{b}/{}", head.size),
                );
            }
            response.body(axum::body::Body::from(bytes)).unwrap()
        }
        Method::DELETE => {
            let response =
                handle_s3_delete(State(st.app.clone()), gateway_headers, Path(path)).await;
            if response.status().is_success() {
                StatusCode::NO_CONTENT.into_response()
            } else {
                map_error(response).await
            }
        }
        _ => {
            let _ = pool;
            err(
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                "unsupported S3 operation",
            )
        }
    }
}
async fn map_error(response: Response) -> Response {
    let status = response.status();
    // Preserve the gateway's machine-readable mutation outcome. Dropping it
    // would tell an S3 client that a failed write is safe to retry.
    let outcome = response.headers().get("x-yadb-write-outcome").cloned();
    let retry = response.headers().get("x-yadb-retry").cloned();
    let content_range = response.headers().get(header::CONTENT_RANGE).cloned();
    let body = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap_or_default();
    let code = match status {
        StatusCode::FORBIDDEN => "AccessDenied",
        StatusCode::NOT_FOUND => "NoSuchKey",
        StatusCode::BAD_REQUEST => "InvalidRequest",
        StatusCode::RANGE_NOT_SATISFIABLE => "InvalidRange",
        _ => "ServiceUnavailable",
    };
    // The gateway answers `{"error": "..."}`. An S3 client reads an S3 message,
    // so unwrap that envelope instead of quoting the JSON into `<Message>`.
    let message = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_owned))
        .unwrap_or_else(|| String::from_utf8_lossy(&body).to_string());
    let mut mapped = err(status, code, &message);
    if let Some(value) = content_range {
        mapped.headers_mut().insert(header::CONTENT_RANGE, value);
    }
    if let Some(value) = outcome {
        mapped.headers_mut().insert("x-yadb-write-outcome", value);
    }
    if let Some(value) = retry {
        mapped.headers_mut().insert("x-yadb-retry", value);
    }
    mapped
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_keeps_plus() {
        assert_eq!(
            query("prefix=a%2Bb&x=hello%20world").unwrap(),
            vec![
                ("prefix".into(), "a+b".into()),
                ("x".into(), "hello world".into())
            ]
        );
    }
    #[test]
    fn xml_escape() {
        assert_eq!(esc("<&\""), "&lt;&amp;&quot;");
    }

    #[tokio::test]
    async fn map_error_preserves_mutation_outcome_headers() {
        let mut response = err(
            StatusCode::SERVICE_UNAVAILABLE,
            "ServiceUnavailable",
            "write outcome unknown",
        );
        response
            .headers_mut()
            .insert("x-yadb-write-outcome", "unknown".parse().unwrap());
        response
            .headers_mut()
            .insert("x-yadb-retry", "do-not-retry".parse().unwrap());
        let mapped = map_error(response).await;
        assert_eq!(mapped.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.headers()["x-yadb-write-outcome"], "unknown");
        assert_eq!(mapped.headers()["x-yadb-retry"], "do-not-retry");
    }

    #[tokio::test]
    async fn map_error_without_outcome_headers_adds_none() {
        let mapped = map_error(err(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "control plane stale",
        ))
        .await;
        assert_eq!(mapped.status(), StatusCode::FORBIDDEN);
        assert!(mapped.headers().get("x-yadb-write-outcome").is_none());
        assert!(mapped.headers().get("x-yadb-retry").is_none());
    }

    fn request_headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, value.parse().unwrap());
        }
        map
    }

    #[test]
    fn a_forwarded_read_keeps_the_local_conditional_contract() {
        // The decision is applied to the ETag the read actually produced, so a
        // concurrent overwrite cannot turn a failed If-Match into a 200.
        assert_eq!(
            evaluate_conditions(&request_headers(&[("if-match", "\"b\"")]), "\"a\"")
                .unwrap()
                .status(),
            StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            evaluate_conditions(&request_headers(&[("if-none-match", "\"a\"")]), "\"a\"")
                .unwrap()
                .status(),
            StatusCode::NOT_MODIFIED
        );
        assert!(evaluate_conditions(&request_headers(&[("if-match", "\"a\"")]), "\"a\"").is_none());
        assert!(
            evaluate_conditions(&request_headers(&[("if-none-match", "\"b\"")]), "\"a\"").is_none()
        );
        assert!(evaluate_conditions(&request_headers(&[]), "\"a\"").is_none());
    }

    #[test]
    fn a_forwarded_read_keeps_the_local_range_and_size_rules() {
        // A clamp is accepted, exactly as the local read answers it.
        assert_eq!(
            preflight_range(&request_headers(&[("range", "bytes=7-999")]), 9).ok(),
            Some(Some((7, 8)))
        );
        // An unsatisfiable range is the local `416` with its Content-Range, not
        // a `503` from the internal gateway.
        let refused = preflight_range(&request_headers(&[("range", "bytes=900-999")]), 9)
            .expect_err("start past the size is unsatisfiable");
        assert_eq!(refused.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(refused.headers()[header::CONTENT_RANGE], "bytes */9");
        // A full read over the wire cap, and a range over it, are the local
        // `400 InvalidRequest` rather than a proxy failure.
        let cap = 16 * 1024 * 1024;
        assert_eq!(
            preflight_range(&request_headers(&[]), cap + 1)
                .expect_err("a full read over 16 MiB is refused")
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            preflight_range(&request_headers(&[("range", "bytes=0-")]), cap + 1)
                .expect_err("a range over 16 MiB is refused")
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(preflight_range(&request_headers(&[]), cap).ok(), Some(None));
        assert_eq!(
            preflight_range(&request_headers(&[("range", "bytes=0-")]), cap).ok(),
            Some(Some((0, cap - 1)))
        );
    }

    #[tokio::test]
    async fn map_error_frames_gateway_range_errors_as_s3_errors() {
        let mut response =
            crate::api_err(StatusCode::RANGE_NOT_SATISFIABLE, "invalid Range header");
        response
            .headers_mut()
            .insert(header::CONTENT_RANGE, "bytes */9".parse().unwrap());
        let mapped = map_error(response).await;
        assert_eq!(mapped.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(mapped.headers()[header::CONTENT_RANGE], "bytes */9");
        let body = axum::body::to_bytes(mapped.into_body(), 65536)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<Code>InvalidRange</Code>"), "{body}");
        // The gateway's JSON envelope is unwrapped, not quoted into <Message>.
        assert!(
            body.contains("<Message>invalid Range header</Message>"),
            "{body}"
        );
        assert!(!body.contains("&quot;"), "{body}");
    }

    #[tokio::test]
    async fn map_error_frames_gateway_bad_requests_as_invalid_request() {
        let mapped = map_error(crate::api_err(
            StatusCode::BAD_REQUEST,
            "use Range for objects over 16 MiB",
        ))
        .await;
        assert_eq!(mapped.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(mapped.into_body(), 65536)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<Code>InvalidRequest</Code>"), "{body}");
        assert!(
            body.contains("<Message>use Range for objects over 16 MiB</Message>"),
            "{body}"
        );
    }
}
