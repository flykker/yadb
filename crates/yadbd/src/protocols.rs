//! Opt-in standard-client frontends. No protocol identity is trusted without a credential.
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::Arc,
    time::Duration,
};

pub mod iscsi;
pub mod pg;
pub mod s3;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub subject: String,
    pub secret: String,
}

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub users: BTreeMap<String, Credential>,
}

impl Credentials {
    pub fn load(path: &Path) -> io::Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        if std::fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::other(
                "credentials file must not be accessible by group/other (chmod 600)",
            ));
        }
        let c: Self = serde_json::from_slice(&std::fs::read(path)?).map_err(io::Error::other)?;
        if c.users.is_empty()
            || c.users.keys().any(|k| {
                k.is_empty()
                    || !k
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            })
            || c.users.values().any(|c| {
                c.subject.is_empty()
                    || !c.subject.bytes().all(|b| (0x21..=0x7e).contains(&b))
                    || c.secret.len() < 12
            })
        {
            return Err(io::Error::other(
                "credentials require nonempty subjects and secrets of at least 12 bytes",
            ));
        }
        Ok(c)
    }
}

pub fn equal(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

pub fn hex(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn unhex(v: &str) -> io::Result<Vec<u8>> {
    if v.len() % 2 != 0 || !v.is_ascii() {
        return Err(io::Error::other("invalid hex"));
    }
    (0..v.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&v[i..i + 2], 16).map_err(io::Error::other))
        .collect()
}
pub fn sha(v: &[u8]) -> String {
    hex(ring::digest::digest(&ring::digest::SHA256, v).as_ref())
}
pub fn hmac(key: &[u8], v: &[u8]) -> Vec<u8> {
    ring::hmac::sign(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key), v)
        .as_ref()
        .to_vec()
}
pub fn random<const N: usize>() -> io::Result<[u8; N]> {
    use ring::rand::SecureRandom;
    let mut v = [0; N];
    ring::rand::SystemRandom::new()
        .fill(&mut v)
        .map_err(|_| io::Error::other("OS randomness unavailable"))?;
    Ok(v)
}

pub fn serve(
    listener: TcpListener,
    handler: impl Fn(TcpStream) -> io::Result<()> + Send + Sync + 'static,
) {
    let handler = Arc::new(handler);
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(conn) = conn else {
                break;
            };
            use std::sync::atomic::Ordering;
            if active.fetch_add(1, Ordering::SeqCst) >= 128 {
                active.fetch_sub(1, Ordering::SeqCst);
                continue;
            }
            let handler = handler.clone();
            let active = active.clone();
            std::thread::spawn(move || {
                let _ = conn.set_read_timeout(Some(Duration::from_secs(120)));
                let _ = conn.set_write_timeout(Some(Duration::from_secs(30)));
                let _ = conn.set_nodelay(true);
                let _ = handler(conn);
                active.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
}

/// Bounded HTTP bridge to existing gateway handlers. Mutations retain their existing leader routing.
pub fn http(
    addr: &str,
    method: &str,
    path: &str,
    subject: &str,
    body: &[u8],
) -> io::Result<(u16, Vec<u8>)> {
    let addr = addr
        .replace("0.0.0.0", "127.0.0.1")
        .replace("[::]", "[::1]");
    let sock = addr.parse().map_err(io::Error::other)?;
    let mut conn = TcpStream::connect_timeout(&sock, Duration::from_secs(2))?;
    conn.set_read_timeout(Some(Duration::from_secs(30)))?;
    conn.set_write_timeout(Some(Duration::from_secs(30)))?;
    if subject.contains(['\r', '\n']) || path.contains(['\r', '\n', ' ']) {
        return Err(io::Error::other("invalid HTTP field"));
    }
    write!(conn,"{method} {path} HTTP/1.1\r\nHost: {addr}\r\nx-yadb-subject: {subject}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len())?;
    conn.write_all(body)?;
    let mut raw = Vec::new();
    conn.take(32 * 1024 * 1024 + 1).read_to_end(&mut raw)?;
    if raw.len() > 32 * 1024 * 1024 {
        return Err(io::Error::other("gateway response too large"));
    }
    let split = raw
        .windows(4)
        .position(|v| v == b"\r\n\r\n")
        .ok_or_else(|| io::Error::other("invalid gateway response"))?;
    let headers = String::from_utf8_lossy(&raw[..split]);
    let code = headers
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::other("missing HTTP status"))?;
    let body = if headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        super::decode_http_chunked(&raw[split + 4..])?
    } else {
        raw[split + 4..].to_vec()
    };
    Ok((code, body))
}

pub fn url(v: &str) -> String {
    v.as_bytes()
        .iter()
        .map(|&b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
pub fn decode(v: &str) -> io::Result<String> {
    let b = v.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            if i + 2 >= b.len() || !b[i + 1].is_ascii_hexdigit() || !b[i + 2].is_ascii_hexdigit() {
                return Err(io::Error::other("invalid URL"));
            }
            out.push(u8::from_str_radix(&v[i + 1..i + 3], 16).map_err(io::Error::other)?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(io::Error::other)
}
