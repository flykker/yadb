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
        let cp = st.app.control.read().unwrap();
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
            let engine = st.app.s3.clone();
            let store = st.store.clone();
            let b = bucket.to_string();
            let k = key.to_string();
            let head =
                match tokio::task::spawn_blocking(move || engine.head_object(pid, &store, &b, &k))
                    .await
                {
                    Ok(Ok(v)) => v,
                    _ => return err(StatusCode::NOT_FOUND, "NoSuchKey", "object not found"),
                };
            if headers
                .get(header::IF_MATCH)
                .is_some_and(|v| v.as_bytes() != head.etag.as_bytes())
            {
                return err(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "ETag mismatch",
                );
            }
            if headers
                .get(header::IF_NONE_MATCH)
                .is_some_and(|v| v.as_bytes() == head.etag.as_bytes())
            {
                return StatusCode::NOT_MODIFIED.into_response();
            }
            let modified =
                chrono::DateTime::from_timestamp(head.last_modified.min(i64::MAX as u64) as i64, 0)
                    .unwrap_or_default()
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string();
            if method == Method::HEAD {
                return Response::builder()
                    .header(header::CONTENT_LENGTH, head.size)
                    .header(header::CONTENT_TYPE, head.content_type)
                    .header(header::ETAG, head.etag)
                    .header(header::LAST_MODIFIED, modified)
                    .header(header::ACCEPT_RANGES, "bytes")
                    .body(axum::body::Body::empty())
                    .unwrap();
            }
            let mut range = None;
            if let Some(v) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
                range = parse_range_header(v, head.size);
                if range.is_none() {
                    let mut r = err(
                        StatusCode::RANGE_NOT_SATISFIABLE,
                        "InvalidRange",
                        "range not satisfiable",
                    );
                    r.headers_mut().insert(
                        header::CONTENT_RANGE,
                        format!("bytes */{}", head.size).parse().unwrap(),
                    );
                    return r;
                }
            }
            if head.size > 16 * 1024 * 1024 && range.is_none() {
                return err(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    "use Range for objects over 16 MiB",
                );
            }
            if let Some((a, b)) = range {
                if b - a + 1 > 16 * 1024 * 1024 {
                    return err(
                        StatusCode::BAD_REQUEST,
                        "InvalidRequest",
                        "range exceeds 16 MiB",
                    );
                }
                gateway_headers.insert(header::RANGE, format!("bytes={a}-{b}").parse().unwrap());
            }
            let mut response =
                handle_s3_get(State(st.app.clone()), gateway_headers, Path(path)).await;
            if response.status().is_success() {
                response
                    .headers_mut()
                    .insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
                response
                    .headers_mut()
                    .insert(header::LAST_MODIFIED, modified.parse().unwrap());
                response
            } else {
                map_error(response).await
            }
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
    let body = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap_or_default();
    err(
        status,
        if status == StatusCode::FORBIDDEN {
            "AccessDenied"
        } else if status == StatusCode::NOT_FOUND {
            "NoSuchKey"
        } else {
            "ServiceUnavailable"
        },
        &String::from_utf8_lossy(&body),
    )
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
}
