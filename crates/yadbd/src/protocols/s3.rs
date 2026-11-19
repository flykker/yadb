//! Path-style AWS S3 API. One listener is bound to a declarative project/ObjectStore.
use super::*;
use crate::*;
use axum::{extract::Request, http::Method};
use base64::Engine;
use serde::Deserialize;

/// The gateway's ListParts JSON, parsed as a typed document. A missing field is
/// a malformed internal answer, not a zero value.
#[derive(Deserialize)]
struct ListPartsBody {
    #[serde(rename = "partNumberMarker")]
    part_number_marker: u32,
    #[serde(rename = "maxParts")]
    max_parts: u32,
    #[serde(rename = "isTruncated")]
    is_truncated: bool,
    #[serde(rename = "nextPartNumberMarker", default)]
    next_part_number_marker: u32,
    parts: Vec<ListedPart>,
}

#[derive(Deserialize)]
struct ListedPart {
    #[serde(rename = "partNumber")]
    part_number: u32,
    etag: String,
    size: u64,
    #[serde(rename = "lastModified")]
    last_modified: u64,
}

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

/// Renders the shared conditional decision as the native wire response.
fn condition_refusal(headers: &HeaderMap, etag: &str) -> Option<Response> {
    match crate::evaluate_conditions(headers, etag) {
        crate::Conditional::Passed => None,
        crate::Conditional::Failed => Some(err(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "ETag mismatch",
        )),
        crate::Conditional::NotModified => Some(StatusCode::NOT_MODIFIED.into_response()),
    }
}

/// Renders the shared range decision as the native wire response.
fn range_slice(headers: &HeaderMap, size: u64) -> Result<Option<(u64, u64)>, Response> {
    let range_hdr = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    match crate::range_decision(range_hdr, size) {
        crate::RangeDecision::Serve(range) => Ok(range),
        crate::RangeDecision::Unsatisfiable => {
            let mut response = err(
                StatusCode::RANGE_NOT_SATISFIABLE,
                "InvalidRange",
                "range not satisfiable",
            );
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                format!("bytes */{size}").parse().unwrap(),
            );
            Err(response)
        }
        crate::RangeDecision::TooLarge => Err(err(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            if range_hdr.is_some() {
                "range exceeds 16 MiB"
            } else {
                "use Range for objects over 16 MiB"
            },
        )),
    }
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
            "consistent",
            "uploads",
            "uploadId",
            "partNumber",
            "part-number-marker",
            "max-parts",
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
            Err(ControlAuthError::NotReady) => {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ServiceUnavailable",
                    CONTROL_NOT_READY,
                )
            }
            Err(ControlAuthError::Stale) => {
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
    if params.contains_key("delete") || headers.contains_key("x-amz-copy-source") {
        return err(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "bulk delete and server-side copy are not supported yet",
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
    // SigV4 proved this subject, so the gateway must be told that it is proven.
    // A daemon started with `--subject-secret` (mandatory in OpenRaft mode)
    // treats an unsigned `x-yadb-subject` as anonymous, which would turn every
    // S3 request into a `403` for `anonymous`. The signature is added for every
    // method, not only keyed mutations.
    if let Some(secret) = st.app.subject_secret.as_deref() {
        match subject_signature(secret, &subject).parse() {
            Ok(v) => {
                gateway_headers.insert("x-yadb-subject-signature", v);
            }
            Err(_) => return err(StatusCode::FORBIDDEN, "AccessDenied", "invalid subject"),
        }
    }
    if let Some(ct) = headers.get(header::CONTENT_TYPE) {
        gateway_headers.insert(header::CONTENT_TYPE, ct.clone());
    }
    // A keyed mutation is scoped by subject and the gateway handlers require a
    // proven identity. The forwarded signature above already vouches for the
    // subject; a keyed request additionally carries its key.
    if (method == Method::PUT || method == Method::DELETE)
        && headers.contains_key("idempotency-key")
    {
        if let Some(value) = headers.get("idempotency-key") {
            gateway_headers.insert("idempotency-key", value.clone());
        }
    }
    let path = (
        st.project.clone(),
        st.store.clone(),
        bucket.to_string(),
        key.to_string(),
    );
    // Multipart uploads are keyed operations on one object; the subresource in
    // the query selects the step. The body has already been authenticated as
    // the signed payload above, and the per-part bound is the 16 MiB buffer the
    // listener enforced before this point.
    if params.contains_key("uploads") || params.contains_key("uploadId") {
        return multipart(&st, &method, &headers, gateway_headers, path, &params, body).await;
    }
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
            if let Some(response) = verify_upload_checksums(&headers, &body) {
                return response;
            }
            let response = handle_s3_put(
                State(st.app.clone()),
                gateway_headers,
                Path(path),
                Query(S3WriteQuery::default()),
                body,
            )
            .await;
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
            // An opt-in quorum-confirmed read must be decided by the gateway: it
            // takes the snapshot on the project-group leader and refuses on
            // legacy metadata. Serving it from the local applied state here would
            // silently return exactly the stale read the option exists to
            // refuse — observed as a `404` for an object that had just been
            // acknowledged through another node.
            if params.get("consistent").is_some_and(|v| *v == "true") {
                // The delegated read must see the client's range and
                // conditional headers, or a consistent read would silently
                // ignore them and answer with the whole object.
                let response = handle_s3_get(
                    method.clone(),
                    State(st.app.clone()),
                    read_headers(&gateway_headers, &headers),
                    Path(path),
                    Query(S3GetQuery { consistent: true }),
                )
                .await;
                if response.status().is_success() {
                    return response;
                }
                // A conditional hit is a successful, bodyless answer, not an
                // error: mapping it would turn 304 into an XML 503.
                if response.status() == StatusCode::NOT_MODIFIED {
                    let mut response = response;
                    response.headers_mut().remove("x-yadb-leader-id");
                    response.headers_mut().remove(header::CONTENT_TYPE);
                    return response;
                }
                return map_error(response).await;
            }
            // Every GET is fenced by a reader lease, and only the metadata
            // group leader can publish one. A follower therefore forwards the
            // request it has already authenticated, together with the `Range`
            // and conditional headers, to the leader's gateway. The leader
            // decides existence, `If-Match`/`If-None-Match`, the range and the
            // 16 MiB rule from the one manifest it reads under its lease, so a
            // follower's lagging metadata cannot refuse or mis-slice a read the
            // leader can serve. On the leader this returns `None` and the read
            // runs locally under the lease taken below. HEAD reads no
            // fragments, so it needs no fence and stays local.
            if method == Method::GET {
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
                forwarded = read_headers(&forwarded, &headers);
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
                    let mut response = response;
                    // The internal hop's identity is not part of the native
                    // wire format, and a 304 has no representation.
                    response.headers_mut().remove("x-yadb-leader-id");
                    if response.status() == StatusCode::NOT_MODIFIED {
                        response.headers_mut().remove(header::CONTENT_TYPE);
                        return response;
                    }
                    // A failure is re-framed as an S3 XML error, because the
                    // gateway answers JSON and a native S3 client parses the
                    // wire format.
                    if response.status().is_success() {
                        return response;
                    }
                    return map_error(response).await;
                }
            }
            let engine = st.app.s3.clone();
            let store = st.store.clone();
            let b = bucket.to_string();
            let k = key.to_string();
            let mut read_lease = if method == Method::GET {
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
            // The lease covers both the manifest read and the chunk fetch, in
            // both metadata modes: the collector consults the same group-scoped
            // reader records on Multi-Raft.
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
            if let Some(response) = condition_refusal(&headers, &head.etag) {
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
            let range = match range_slice(&headers, head.size) {
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
            let response = handle_s3_delete(
                State(st.app.clone()),
                gateway_headers,
                Path(path),
                Query(S3WriteQuery::default()),
            )
            .await;
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
/// Escapes XML *text* (not attributes): a quoted ETag stays readable as `"`.
fn esc_text(v: &str) -> String {
    v.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Renders a gateway multipart failure as the S3 XML error document.
///
/// The gateway answers JSON carrying the S3 error `code`, so the wire layer
/// never has to guess a code from prose. A response without one (a control
/// plane refusal, for instance) keeps the same status-based fallback `map_error`
/// uses, and the explicit no-retry marker is preserved.
async fn multipart_error(response: Response) -> Response {
    let status = response.status();
    let retry = response.headers().get("x-yadb-retry").cloned();
    // A forwarded mutation classified as `unknown` must keep that machine-
    // readable classification; dropping it would tell the client a failed
    // write is safe to replay.
    let outcome = response.headers().get("x-yadb-write-outcome").cloned();
    let body = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap_or_default();
    let parsed = serde_json::from_slice::<serde_json::Value>(&body).ok();
    let message = parsed
        .as_ref()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_owned))
        .unwrap_or_else(|| String::from_utf8_lossy(&body).to_string());
    let code = parsed
        .as_ref()
        .and_then(|v| v.get("code").and_then(|c| c.as_str()).map(str::to_owned))
        .unwrap_or_else(|| match status {
            StatusCode::NOT_FOUND => "NoSuchUpload".into(),
            StatusCode::BAD_REQUEST => "InvalidRequest".into(),
            StatusCode::FORBIDDEN => "AccessDenied".into(),
            StatusCode::SERVICE_UNAVAILABLE => "ServiceUnavailable".into(),
            _ => "InternalError".into(),
        });
    let mut mapped = err(status, &code, &message);
    if let Some(value) = outcome {
        mapped.headers_mut().insert("x-yadb-write-outcome", value);
    }
    if let Some(value) = retry {
        mapped.headers_mut().insert("x-yadb-retry", value);
    }
    mapped
}

/// Dispatches the multipart subresources of a keyed request.
///
/// Upload state and ETags belong to the engine; this function only selects the
/// operation, bounds the part, and renders the S3 XML documents. Every write is
/// executed by the same gateway handler a `/s3/...` request uses, so leader
/// forwarding, authorization and error semantics are shared with single PUT.
async fn multipart(
    st: &S3State,
    method: &Method,
    headers: &HeaderMap,
    gateway: HeaderMap,
    path: (String, String, String, String),
    params: &BTreeMap<&str, &str>,
    body: axum::body::Bytes,
) -> Response {
    let (project, store, bucket, key) = path;
    if key.is_empty() {
        return err(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "multipart operations require an object key",
        );
    }
    let upload_id = params.get("uploadId").map(|v| (*v).to_string());
    let part_number = match params.get("partNumber") {
        Some(v) => match v.parse::<u32>() {
            Ok(v) => Some(v),
            Err(_) => {
                return err(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "partNumber must be an integer",
                )
            }
        },
        None => None,
    };
    // ListParts pagination. AWS bounds `max-parts` to 1..=1000. Presence with an
    // unparsable value — including an empty one — is refused rather than
    // silently replaced: `?max-parts=` is a client error, not an absent page
    // size, and treating it as 1000 would answer a different request than the
    // one sent.
    let part_number_marker = match params.get("part-number-marker") {
        Some(v) => match v.parse::<u32>() {
            Ok(v) => v,
            Err(_) => {
                return err(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "part-number-marker must be an integer",
                )
            }
        },
        None => 0,
    };
    let max_parts = match params.get("max-parts") {
        Some(v) => match v.parse::<u32>() {
            Ok(v) if (1..=1000).contains(&v) => v,
            _ => {
                return err(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "max-parts must be an integer between 1 and 1000",
                )
            }
        },
        None => 1000,
    };
    // A gateway answer is internal JSON. It is read fallibly and bounded: a
    // malformed or truncated body must surface as an error, never as an empty
    // successful result.
    let json = |response: Response| async move {
        let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .map_err(|_| ())?;
        serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|_| ())
    };
    match (method, params.contains_key("uploads"), upload_id) {
        (&Method::POST, true, _) => {
            // The gateway receives only the headers this listener chose to
            // carry, so an option such as `x-amz-meta-*` would be invisible to
            // `handle_s3_create_multipart` and the client's metadata would be
            // silently dropped. Refuse on the raw client headers, before any
            // durable upload record exists.
            if let Some(reason) = unsupported_object_options(headers) {
                return err(StatusCode::NOT_IMPLEMENTED, "NotImplemented", reason);
            }
            let response = handle_s3_create_multipart(
                st.app.clone(),
                gateway,
                project,
                store,
                bucket.clone(),
                key.clone(),
            )
            .await;
            if !response.status().is_success() {
                return multipart_error(response).await;
            }
            let listed = match json(response).await {
                Ok(value) => value,
                Err(()) => {
                    return err(
                        StatusCode::BAD_GATEWAY,
                        "InternalError",
                        "initiate returned a malformed response",
                    )
                }
            };
            let Some(upload_id) = listed
                .get("uploadId")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
            else {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ServiceUnavailable",
                    "initiate did not return an upload id",
                );
            };
            xml(
                StatusCode::OK,
                format!(
                    "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                    esc(&bucket),
                    esc(&key),
                    esc(&upload_id)
                ),
            )
        }
        (&Method::PUT, _, Some(upload_id)) => {
            let Some(part_number) = part_number else {
                return err(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "partNumber is required with uploadId",
                );
            };
            // A part is verified exactly like a single PUT body: the client's
            // `Content-MD5`/`x-amz-checksum-*` is checked before any fragment is
            // written.
            if let Some(response) = verify_upload_checksums(headers, &body) {
                return response;
            }
            let response = handle_s3_upload_part(
                st.app.clone(),
                gateway,
                project,
                store,
                bucket,
                key,
                upload_id,
                part_number,
                body,
            )
            .await;
            if !response.status().is_success() {
                return multipart_error(response).await;
            }
            let mut ok = StatusCode::OK.into_response();
            if let Some(value) = response.headers().get(header::ETAG) {
                ok.headers_mut().insert(header::ETAG, value.clone());
            }
            // S3 echoes the checksum it was asked to verify; a client that sent
            // one expects it back on the part response.
            for name in ["x-amz-checksum-crc32", "x-amz-checksum-sha256"] {
                if let Some(value) = headers.get(name) {
                    if let Ok(name) = axum::http::HeaderName::from_bytes(name.as_bytes()) {
                        ok.headers_mut().insert(name, value.clone());
                    }
                }
            }
            ok
        }
        (&Method::POST, _, Some(upload_id)) => {
            let response = handle_s3_complete_multipart(
                st.app.clone(),
                gateway,
                project,
                store,
                bucket.clone(),
                key.clone(),
                upload_id.clone(),
                body,
            )
            .await;
            if !response.status().is_success() {
                return multipart_error(response).await;
            }
            let listed = match json(response).await {
                Ok(value) => value,
                Err(()) => {
                    return err(
                        StatusCode::BAD_GATEWAY,
                        "InternalError",
                        "completion returned a malformed response",
                    )
                }
            };
            let Some(etag) = listed
                .get("etag")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
            else {
                return err(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ServiceUnavailable",
                    "completion did not return an object ETag",
                );
            };
            let host = headers
                .get(header::HOST)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("yadb");
            xml(
                StatusCode::OK,
                format!(
                    "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Location>http://{}/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                    esc(host),
                    esc(&bucket),
                    esc(&key),
                    esc(&bucket),
                    esc(&key),
                    esc_text(&etag)
                ),
            )
        }
        (&Method::DELETE, _, Some(upload_id)) => {
            let response = handle_s3_abort_multipart(
                st.app.clone(),
                gateway,
                project,
                store,
                bucket,
                key,
                upload_id,
            )
            .await;
            if !response.status().is_success() {
                return multipart_error(response).await;
            }
            StatusCode::NO_CONTENT.into_response()
        }
        (&Method::GET, _, Some(upload_id)) => {
            let response = handle_s3_list_parts(
                st.app.clone(),
                gateway,
                project,
                store,
                bucket.clone(),
                key.clone(),
                upload_id.clone(),
                part_number_marker,
                max_parts,
            )
            .await;
            if !response.status().is_success() {
                return multipart_error(response).await;
            }
            let listed = match json(response).await {
                Ok(value) => value,
                Err(()) => {
                    return err(
                        StatusCode::BAD_GATEWAY,
                        "InternalError",
                        "list-parts returned a malformed response",
                    )
                }
            };
            let listed = match serde_json::from_value::<ListPartsBody>(listed) {
                Ok(listed) => listed,
                Err(_) => {
                    return err(
                        StatusCode::BAD_GATEWAY,
                        "InternalError",
                        "list-parts response did not have the expected shape",
                    )
                }
            };
            let mut out = format!(
                "<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId><PartNumberMarker>{}</PartNumberMarker><MaxParts>{}</MaxParts><IsTruncated>{}</IsTruncated>",
                esc(&bucket),
                esc(&key),
                esc(&upload_id),
                listed.part_number_marker,
                listed.max_parts,
                listed.is_truncated
            );
            for part in &listed.parts {
                out.push_str(&format!(
                    "<Part><PartNumber>{}</PartNumber><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size></Part>",
                    part.part_number,
                    date(part.last_modified),
                    esc_text(&format!("\"{}\"", part.etag.trim_matches('"'))),
                    part.size
                ));
            }
            if listed.is_truncated {
                out.push_str(&format!(
                    "<NextPartNumberMarker>{}</NextPartNumberMarker>",
                    listed.next_part_number_marker
                ));
            }
            out.push_str("</ListPartsResult>");
            xml(StatusCode::OK, out)
        }
        _ => err(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "unsupported multipart operation",
        ),
    }
}

/// Verifies the integrity headers a client attached to an upload body.
///
/// A part is a whole object fragment as far as the wire is concerned, so the
/// same rule applies to `PUT` and `UploadPart`: a declared digest that does not
/// match the bytes the listener received is `BadDigest`, and an algorithm this
/// build cannot verify is refused rather than silently ignored. `None` means the
/// body may be stored.
fn verify_upload_checksums(headers: &HeaderMap, body: &[u8]) -> Option<Response> {
    for name in [
        "x-amz-server-side-encryption",
        "x-amz-acl",
        "x-amz-checksum-crc32c",
        "x-amz-checksum-crc64nvme",
        "x-amz-checksum-sha1",
    ] {
        if headers.contains_key(name) {
            return Some(err(
                StatusCode::NOT_IMPLEMENTED,
                "NotImplemented",
                "requested encryption, ACL or checksum is unsupported",
            ));
        }
    }
    for (name, value) in [
        ("content-md5", super::iscsi::md5(body).to_vec()),
        (
            "x-amz-checksum-crc32",
            crc32fast::hash(body).to_be_bytes().to_vec(),
        ),
        (
            "x-amz-checksum-sha256",
            ring::digest::digest(&ring::digest::SHA256, body)
                .as_ref()
                .to_vec(),
        ),
    ] {
        if let Some(got) = headers.get(name) {
            let expected = base64::engine::general_purpose::STANDARD.encode(value);
            if got.as_bytes() != expected.as_bytes() {
                return Some(err(
                    StatusCode::BAD_REQUEST,
                    "BadDigest",
                    "checksum mismatch",
                ));
            }
        }
    }
    None
}

/// The gateway headers for a read: the authenticated subject the caller already
/// built, plus every `Range` and conditional field line the client sent.
///
/// `Range` is single-valued; a conditional header may arrive as several field
/// lines, and dropping one would give the leader a weaker condition than the
/// caller sent. The base map is fresh per request, so every line is added once.
fn read_headers(base: &HeaderMap, headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = base.clone();
    for name in [header::RANGE, header::IF_MATCH, header::IF_NONE_MATCH] {
        for value in headers.get_all(&name) {
            forwarded.append(name.clone(), value.clone());
        }
    }
    forwarded
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
        StatusCode::PRECONDITION_FAILED => "PreconditionFailed",
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
        // An ETag is text, not an attribute: its quotes stay literal so a
        // client reading `<ETag>"abc-2"</ETag>` sees the same bytes S3 sends.
        assert_eq!(esc_text("\"abc-2\" & <x>"), "\"abc-2\" &amp; &lt;x&gt;");
    }

    #[test]
    fn upload_checksums_are_verified_for_a_whole_body_or_a_part() {
        let body = b"part-bytes";
        let md5 = base64::engine::general_purpose::STANDARD.encode(super::iscsi::md5(body));
        let crc32 =
            base64::engine::general_purpose::STANDARD.encode(crc32fast::hash(body).to_be_bytes());
        let with = |name: &'static str, value: &str| {
            let mut map = HeaderMap::new();
            map.insert(name, value.parse().unwrap());
            map
        };
        assert!(verify_upload_checksums(&HeaderMap::new(), body).is_none());
        assert!(verify_upload_checksums(&with("content-md5", &md5), body).is_none());
        assert!(verify_upload_checksums(&with("x-amz-checksum-crc32", &crc32), body).is_none());
        // A digest for different bytes, and an algorithm this build cannot
        // verify, are both refused before anything is stored.
        let bad = with("content-md5", "AAAAAAAAAAAAAAAAAAAAAA==");
        assert_eq!(
            verify_upload_checksums(&bad, body).unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        let unsupported = with("x-amz-checksum-crc32c", "AAAAAA==");
        assert_eq!(
            verify_upload_checksums(&unsupported, body)
                .unwrap()
                .status(),
            StatusCode::NOT_IMPLEMENTED
        );
    }

    #[tokio::test]
    async fn multipart_errors_render_s3_codes_from_the_gateway_envelope() {
        // The gateway names the S3 code; the wire layer must not invent one.
        let gateway = (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "part 2: ETag mismatch",
                "code": "InvalidPart",
            })),
        )
            .into_response();
        let mapped = multipart_error(gateway).await;
        assert_eq!(mapped.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(mapped.into_body(), 65536)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<Code>InvalidPart</Code>"), "{body}");
        assert!(
            body.contains("<Message>part 2: ETag mismatch</Message>"),
            "{body}"
        );

        // A refusal without a code keeps the status-based fallback, and the
        // no-retry marker survives so a client does not replay a completion
        // whose outcome is ambiguous.
        let mut ambiguous = crate::api_err(StatusCode::SERVICE_UNAVAILABLE, "state changed");
        ambiguous
            .headers_mut()
            .insert("x-yadb-retry", "do-not-retry".parse().unwrap());
        ambiguous
            .headers_mut()
            .insert("x-yadb-write-outcome", "unknown".parse().unwrap());
        let mapped = multipart_error(ambiguous).await;
        assert_eq!(mapped.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(mapped.headers()["x-yadb-retry"], "do-not-retry");
        // The machine-readable outcome must survive the wire-format conversion;
        // dropping it would tell the client the failed write is safe to replay.
        assert_eq!(mapped.headers()["x-yadb-write-outcome"], "unknown");
        let body = axum::body::to_bytes(mapped.into_body(), 65536)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<Code>ServiceUnavailable</Code>"), "{body}");
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
    fn a_read_keeps_the_subject_and_every_conditional_line() {
        // A consistent read is delegated to the gateway handler, so the headers
        // it would otherwise decide on must travel with it. Two `If-Match` field
        // lines are two conditions, not one; collapsing them would make the read
        // pass a check the client asked to fail.
        let base = request_headers(&[("x-yadb-subject", "sa:billing-backend")]);
        let mut incoming = request_headers(&[("range", "bytes=0-9"), ("if-match", "\"a\"")]);
        incoming.append("if-match", "\"b\"".parse().unwrap());
        let out = read_headers(&base, &incoming);
        assert_eq!(out["x-yadb-subject"], "sa:billing-backend");
        assert_eq!(out["range"], "bytes=0-9");
        assert_eq!(out.get_all("if-match").iter().count(), 2);
    }

    #[test]
    fn a_read_applies_the_local_conditional_contract() {
        // The decision is shared by the native listener and the gateway, and is
        // applied to the ETag of the manifest the bytes came from, so a
        // concurrent overwrite cannot turn a failed If-Match into a 200.
        assert!(matches!(
            crate::evaluate_conditions(&request_headers(&[("if-match", "\"b\"")]), "\"a\""),
            crate::Conditional::Failed
        ));
        assert!(matches!(
            crate::evaluate_conditions(&request_headers(&[("if-none-match", "\"a\"")]), "\"a\""),
            crate::Conditional::NotModified
        ));
        assert!(matches!(
            crate::evaluate_conditions(&request_headers(&[("if-match", "\"a\"")]), "\"a\""),
            crate::Conditional::Passed
        ));
        assert!(matches!(
            crate::evaluate_conditions(&request_headers(&[("if-none-match", "\"b\"")]), "\"a\""),
            crate::Conditional::Passed
        ));
        assert!(matches!(
            crate::evaluate_conditions(&request_headers(&[]), "\"a\""),
            crate::Conditional::Passed
        ));
        // `*` and a list are valid values the wire handler must honour too.
        assert!(matches!(
            crate::evaluate_conditions(&request_headers(&[("if-none-match", "*")]), "\"a\""),
            crate::Conditional::NotModified
        ));
        assert!(matches!(
            crate::evaluate_conditions(
                &request_headers(&[("if-match", "\"old\", \"a\"")]),
                "\"a\""
            ),
            crate::Conditional::Passed
        ));
        // The native rendering of that decision.
        assert_eq!(
            condition_refusal(&request_headers(&[("if-match", "\"b\"")]), "\"a\"")
                .unwrap()
                .status(),
            StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            condition_refusal(&request_headers(&[("if-none-match", "\"a\"")]), "\"a\"")
                .unwrap()
                .status(),
            StatusCode::NOT_MODIFIED
        );
        assert!(condition_refusal(&request_headers(&[]), "\"a\"").is_none());
    }

    #[test]
    fn a_read_applies_the_local_range_and_size_rules() {
        // A clamp is accepted, exactly as the local read answers it.
        assert_eq!(
            range_slice(&request_headers(&[("range", "bytes=7-999")]), 9).ok(),
            Some(Some((7, 8)))
        );
        // An unsatisfiable range is the local `416` with its Content-Range, not
        // a `503` from the internal gateway.
        let refused = range_slice(&request_headers(&[("range", "bytes=900-999")]), 9)
            .expect_err("start past the size is unsatisfiable");
        assert_eq!(refused.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(refused.headers()[header::CONTENT_RANGE], "bytes */9");
        // A full read over the wire cap, and a range over it, are the local
        // `400 InvalidRequest` rather than a proxy failure.
        let cap = crate::S3_READ_CAP;
        let full = range_slice(&request_headers(&[]), cap + 1)
            .expect_err("a full read over 16 MiB is refused");
        assert_eq!(full.status(), StatusCode::BAD_REQUEST);
        let ranged = range_slice(&request_headers(&[("range", "bytes=0-")]), cap + 1)
            .expect_err("a range over 16 MiB is refused");
        assert_eq!(ranged.status(), StatusCode::BAD_REQUEST);
        assert_eq!(range_slice(&request_headers(&[]), cap).ok(), Some(None));
        assert_eq!(
            range_slice(&request_headers(&[("range", "bytes=0-")]), cap).ok(),
            Some(Some((0, cap - 1)))
        );
        // The shared decision itself, including the empty-object case.
        assert!(matches!(
            crate::range_decision(Some("bytes=0-0"), 0),
            crate::RangeDecision::Unsatisfiable
        ));
        assert!(matches!(
            crate::range_decision(Some("items=0-1"), 9),
            crate::RangeDecision::Unsatisfiable
        ));
    }

    #[tokio::test]
    async fn map_error_frames_gateway_range_errors_as_s3_errors() {
        let mut response =
            crate::api_err(StatusCode::RANGE_NOT_SATISFIABLE, "range not satisfiable");
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
            body.contains("<Message>range not satisfiable</Message>"),
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
