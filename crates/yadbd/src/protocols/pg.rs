//! PostgreSQL v3 wire frontend: Simple Query, the extended query protocol
//! (Parse/Bind/Describe/Execute/Close/Flush/Sync) and `COPY` in text format,
//! over cleartext or server-side TLS. Text format only.
//!
//! The engine does not type parameters. `Bind` decodes text values using the OIDs
//! supplied by `Parse` and substitutes them into the SQL text as literals. OID 0 is
//! inferred as integer, then float, then boolean, then text. The substitution is
//! quote/comment aware so a parameter value can never become SQL syntax.
use super::*;
use crate::AppState;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use yadb_db::{
    eval_scalar_select, parse_sql_script, parse_sql_value, setting_default, QueryResult,
    ScalarContext, SqlStmt, SqlValue,
};

/// A client connection. `SSLRequest` upgrades a plain socket in place; every
/// message helper is written against this type so the protocol code is the same
/// on both transports.
enum Stream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ServerConnection, TcpStream>>),
}
impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(c) => c.read(buf),
            Stream::Tls(c) => c.read(buf),
        }
    }
}
impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(c) => c.write(buf),
            Stream::Tls(c) => c.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(c) => c.flush(),
            Stream::Tls(c) => c.flush(),
        }
    }
}
impl Stream {
    /// Completes the server handshake on `c` and returns the encrypted stream.
    /// The handshake is driven explicitly so a rejected certificate is reported
    /// before the startup packet is read, not as a mysterious frame error.
    fn upgrade(c: Stream, config: &Arc<rustls::ServerConfig>) -> io::Result<Stream> {
        let Stream::Plain(mut sock) = c else {
            return Err(io::Error::other("TLS is already active"));
        };
        let mut conn = rustls::ServerConnection::new(config.clone()).map_err(io::Error::other)?;
        let mut rounds = 0u32;
        while conn.is_handshaking() {
            let (read, written) = conn.complete_io(&mut sock)?;
            if read == 0 && written == 0 {
                return Err(io::Error::other("TLS handshake made no progress"));
            }
            rounds += 1;
            if rounds > 64 {
                return Err(io::Error::other("TLS handshake did not finish"));
            }
        }
        Ok(Stream::Tls(Box::new(rustls::StreamOwned::new(conn, sock))))
    }
}

/// Server-side TLS for the pgwire listener.
///
/// It is configured only when both a certificate chain and a private key are
/// given. Without it the listener keeps the legacy cleartext behaviour; with it
/// `SSLRequest` is answered `S` and a plain startup packet is still accepted
/// unless `required` is set, so existing clients keep working after a TLS
/// rollout and can be forced to encrypt once they are reconfigured.
pub struct Tls {
    config: Arc<rustls::ServerConfig>,
    required: bool,
}

impl Tls {
    pub fn load(
        cert: &Path,
        key: &Path,
        client_ca: Option<&Path>,
        required: bool,
    ) -> io::Result<Self> {
        const MAX_TLS_FILE: u64 = 256 * 1024;
        let read = |path: &Path| -> io::Result<Vec<u8>> {
            let meta = std::fs::metadata(path)?;
            if meta.len() > MAX_TLS_FILE {
                return Err(io::Error::other(format!(
                    "{} is larger than {MAX_TLS_FILE} bytes",
                    path.display()
                )));
            }
            std::fs::read(path)
        };
        let chain = certificates(&read(cert)?)?;
        if chain.is_empty() {
            return Err(io::Error::other("TLS certificate chain is empty"));
        }
        let key = private_key(&read(key)?)?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(io::Error::other)?;
        let builder = match client_ca {
            Some(path) => {
                let mut roots = rustls::RootCertStore::empty();
                for ca in certificates(&read(path)?)? {
                    roots.add(ca).map_err(io::Error::other)?;
                }
                if roots.is_empty() {
                    return Err(io::Error::other("TLS client CA bundle is empty"));
                }
                let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                    Arc::new(roots),
                    provider,
                )
                .build()
                .map_err(io::Error::other)?;
                builder.with_client_cert_verifier(verifier)
            }
            None => builder.with_no_client_auth(),
        };
        let config = builder
            .with_single_cert(chain, key)
            .map_err(io::Error::other)?;
        Ok(Self {
            config: Arc::new(config),
            required,
        })
    }
}

/// PEM when the file starts with a PEM marker, DER otherwise, so the test
/// fixtures and certificates produced by `openssl` both load unchanged.
fn certificates(bytes: &[u8]) -> io::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    if bytes.starts_with(b"-----BEGIN") {
        rustls::pki_types::CertificateDer::pem_slice_iter(bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(io::Error::other)
    } else {
        Ok(vec![bytes.to_vec().into()])
    }
}

fn private_key(bytes: &[u8]) -> io::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    if bytes.starts_with(b"-----BEGIN") {
        rustls::pki_types::PrivateKeyDer::from_pem_slice(bytes).map_err(io::Error::other)
    } else {
        rustls::pki_types::PrivateKeyDer::try_from(bytes.to_vec()).map_err(io::Error::other)
    }
}

/// Bounds on session state; exceeding either answers a clean error instead of growing.
const MAX_PREPARED: usize = 64;
const MAX_PORTALS: usize = 64;
const MAX_SETTINGS: usize = 256;
const MAX_NAME: usize = 256;
const MAX_SQL: usize = 64 * 1024;
const MAX_PARAMS: u32 = 32_767;
/// Bounds on one `COPY` line and on one generated `INSERT` batch.
const MAX_COPY_LINE: usize = 1024 * 1024;
const COPY_BATCH_ROWS: usize = 500;
const COPY_BATCH_BYTES: usize = 32 * 1024;

fn packet(c: &mut Stream, kind: u8, data: &[u8]) -> io::Result<()> {
    c.write_all(&[kind])?;
    c.write_all(&((data.len() + 4) as u32).to_be_bytes())?;
    c.write_all(data)
}
fn error(c: &mut Stream, code: &str, msg: &str) -> io::Result<()> {
    packet(
        c,
        b'E',
        format!("SERROR\0VERROR\0C{code}\0M{}\0\0", msg.replace('\0', "")).as_bytes(),
    )
}
fn ready(c: &mut Stream, status: u8) -> io::Result<()> {
    packet(c, b'Z', &[status])
}
fn frame(c: &mut Stream) -> io::Result<Vec<u8>> {
    let mut n = [0; 4];
    c.read_exact(&mut n)?;
    let n = u32::from_be_bytes(n) as usize;
    if !(4..=1024 * 1024).contains(&n) {
        return Err(io::Error::other("invalid PostgreSQL frame size"));
    }
    let mut data = vec![0; n - 4];
    c.read_exact(&mut data)?;
    Ok(data)
}
fn cstr(v: &[u8]) -> io::Result<&str> {
    if v.last() != Some(&0) {
        return Err(io::Error::other("missing string terminator"));
    }
    std::str::from_utf8(&v[..v.len() - 1]).map_err(io::Error::other)
}

fn row_description(columns: &[String]) -> Vec<u8> {
    let mut desc = Vec::new();
    desc.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for name in columns {
        desc.extend_from_slice(name.as_bytes());
        desc.push(0);
        desc.extend_from_slice(&0u32.to_be_bytes());
        desc.extend_from_slice(&0u16.to_be_bytes());
        desc.extend_from_slice(&25u32.to_be_bytes()); // text; avoids type inference from an empty result
        desc.extend_from_slice(&(-1i16).to_be_bytes());
        desc.extend_from_slice(&(-1i32).to_be_bytes());
        desc.extend_from_slice(&0u16.to_be_bytes());
    }
    desc
}
fn data_rows(c: &mut Stream, res: &QueryResult) -> io::Result<()> {
    for row in &res.rows {
        let mut data = Vec::new();
        data.extend_from_slice(&(row.len() as u16).to_be_bytes());
        for val in row {
            let s = match val {
                SqlValue::Null => None,
                SqlValue::Text(v) => Some(v.clone()),
                SqlValue::Int(v) => Some(v.to_string()),
                SqlValue::Bool(v) => Some(if *v { "t" } else { "f" }.to_string()),
            };
            if let Some(s) = s {
                data.extend_from_slice(&(s.len() as i32).to_be_bytes());
                data.extend_from_slice(s.as_bytes());
            } else {
                data.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
        packet(c, b'D', &data)?;
    }
    Ok(())
}
/// Simple Query result: RowDescription (when there are columns) followed by DataRows.
fn rows(c: &mut Stream, res: &QueryResult) -> io::Result<()> {
    if !res.columns.is_empty() {
        packet(c, b'T', &row_description(&res.columns))?;
    }
    data_rows(c, res)
}

/// A protocol error that must be reported as an ErrorResponse.
#[derive(Debug)]
struct PgError {
    code: &'static str,
    message: String,
}
impl PgError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Cursor over one frontend message body.
struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], PgError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.data.len())
            .ok_or_else(|| PgError::new("08P01", "invalid frontend message"))?;
        let out = &self.data[self.pos..end];
        self.pos = end;
        Ok(out)
    }
    fn u16(&mut self) -> Result<u16, PgError> {
        let v = self.take(2)?;
        Ok(u16::from_be_bytes([v[0], v[1]]))
    }
    fn i32(&mut self) -> Result<i32, PgError> {
        let v = self.take(4)?;
        Ok(i32::from_be_bytes([v[0], v[1], v[2], v[3]]))
    }
    fn u32(&mut self) -> Result<u32, PgError> {
        let v = self.take(4)?;
        Ok(u32::from_be_bytes([v[0], v[1], v[2], v[3]]))
    }
    fn cstr(&mut self) -> Result<&'a str, PgError> {
        let rest = &self.data[self.pos..];
        let end = rest
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| PgError::new("08P01", "unterminated string in frontend message"))?;
        let out = std::str::from_utf8(&rest[..end])
            .map_err(|_| PgError::new("22021", "invalid UTF-8 in frontend message"))?;
        self.pos += end + 1;
        Ok(out)
    }
}

/// One `$n` occurrence in the statement text.
#[derive(Debug, PartialEq, Eq)]
struct Placeholder {
    start: usize,
    end: usize,
    index: u32,
}
#[derive(Debug, Default, PartialEq, Eq)]
struct Scan {
    placeholders: Vec<Placeholder>,
    statements: usize,
}

/// Find `$n` placeholders outside string literals, quoted identifiers and comments.
///
/// Also counts top-level statements so `Parse` can reject multi-statement strings.
/// Dollar-quoted strings are not special-cased: the engine does not support them and
/// rejects the resulting text, so no placeholder can escape from one.
fn scan(sql: &str) -> Result<Scan, PgError> {
    let b = sql.as_bytes();
    let mut out = Scan::default();
    let mut content = false;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'\'' => {
                content = true;
                i += 1;
                loop {
                    if i >= b.len() {
                        return Err(PgError::new("42601", "unterminated string literal"));
                    }
                    if b[i] == b'\'' {
                        if b.get(i + 1) == Some(&b'\'') {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'"' => {
                content = true;
                i += 1;
                loop {
                    if i >= b.len() {
                        return Err(PgError::new("42601", "unterminated quoted identifier"));
                    }
                    if b[i] == b'"' {
                        if b.get(i + 1) == Some(&b'"') {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                // PostgreSQL block comments nest.
                i += 2;
                let mut depth = 1u32;
                while depth > 0 {
                    if i >= b.len() {
                        return Err(PgError::new("42601", "unterminated /* comment"));
                    }
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            b'$' if b.get(i + 1).is_some_and(|v| v.is_ascii_digit()) => {
                content = true;
                let start = i;
                i += 1;
                let mut index: u32 = 0;
                while i < b.len() && b[i].is_ascii_digit() {
                    index = index
                        .saturating_mul(10)
                        .saturating_add(u32::from(b[i] - b'0'));
                    i += 1;
                }
                if index == 0 || index > MAX_PARAMS {
                    return Err(PgError::new(
                        "42P02",
                        format!("there is no parameter ${index}"),
                    ));
                }
                out.placeholders.push(Placeholder {
                    start,
                    end: i,
                    index,
                });
            }
            b';' => {
                if content {
                    out.statements += 1;
                    content = false;
                }
                i += 1;
            }
            c if c.is_ascii_whitespace() => i += 1,
            _ => {
                content = true;
                i += 1;
            }
        }
    }
    if content {
        out.statements += 1;
    }
    Ok(out)
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Accept only a plain decimal integer that fits the OID's range.
fn integer_literal(value: &str, oid: u32) -> Result<String, PgError> {
    let valid = match oid {
        21 => value.parse::<i16>().is_ok(),
        23 => value.parse::<i32>().is_ok(),
        26 => value.parse::<u32>().is_ok(),
        _ => value.parse::<i64>().is_ok(),
    };
    if valid {
        Ok(value.to_string())
    } else {
        Err(PgError::new(
            "22P02",
            format!("invalid input syntax for type integer: \"{value}\""),
        ))
    }
}

/// Plain decimal or exponent form; rejects `inf`, `NaN`, hex and whitespace.
fn is_float(value: &str) -> bool {
    let b = value.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    let mut digits = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        digits += 1;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return false;
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let mut exponent = 0;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            exponent += 1;
        }
        if exponent == 0 {
            return false;
        }
    }
    i == b.len()
}

fn float_literal(value: &str) -> Result<String, PgError> {
    if is_float(value) {
        Ok(value.to_string())
    } else {
        Err(PgError::new(
            "22P02",
            format!("invalid input syntax for type double precision: \"{value}\""),
        ))
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Some(true),
        "false" | "f" | "0" => Some(false),
        _ => None,
    }
}

fn bool_literal(value: &str) -> Result<String, PgError> {
    parse_bool(value)
        .map(|v| if v { "true" } else { "false" }.to_string())
        .ok_or_else(|| {
            PgError::new(
                "22P02",
                format!("invalid input syntax for type boolean: \"{value}\""),
            )
        })
}

/// Turn one text-format Bind value into a SQL literal.
///
/// OID 0 is inferred as integer, then float, then boolean, then text. This is a
/// deliberate limitation: a real server types parameters and does not guess.
fn literal(value: Option<&str>, oid: u32) -> Result<String, PgError> {
    let Some(value) = value else {
        return Ok("NULL".into());
    };
    if value.contains('\0') {
        return Err(PgError::new("22021", "NUL byte in text parameter"));
    }
    match oid {
        16 => bool_literal(value),
        20 | 21 | 23 | 26 => integer_literal(value, oid),
        700 | 701 => float_literal(value),
        0 => {
            if value.parse::<i64>().is_ok() {
                Ok(value.to_string())
            } else if is_float(value) {
                Ok(value.to_string())
            } else if let Some(b) = parse_bool(value) {
                Ok(if b { "true" } else { "false" }.into())
            } else {
                Ok(quote(value))
            }
        }
        _ => Ok(quote(value)),
    }
}

/// Substitute every `$n` with a literal built from the bound text values.
fn substitute(sql: &str, values: &[Option<&str>], oids: &[u32]) -> Result<String, PgError> {
    let scan = scan(sql)?;
    let required = scan.placeholders.iter().map(|p| p.index).max().unwrap_or(0);
    if values.len() as u32 > required {
        return Err(PgError::new(
            "08P01",
            format!(
                "bind message supplies {} parameters, but prepared statement requires {required}",
                values.len()
            ),
        ));
    }
    if (values.len() as u32) < required {
        return Err(PgError::new(
            "42P02",
            format!("there is no parameter ${}", values.len() + 1),
        ));
    }
    let mut out = String::with_capacity(sql.len());
    let mut last = 0;
    for p in &scan.placeholders {
        out.push_str(&sql[last..p.start]);
        let index = p.index as usize - 1;
        out.push_str(&literal(
            values[index],
            oids.get(index).copied().unwrap_or(0),
        )?);
        last = p.end;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// Replace every placeholder with `NULL`; used only to obtain column metadata.
fn substitute_nulls(sql: &str) -> Result<String, PgError> {
    let scan = scan(sql)?;
    let mut out = String::with_capacity(sql.len() + scan.placeholders.len() * 4);
    let mut last = 0;
    for p in &scan.placeholders {
        out.push_str(&sql[last..p.start]);
        out.push_str("NULL");
        last = p.end;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// Renders one already-parsed SQL literal back into SQL text.
fn literal_sql(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "NULL".into(),
        SqlValue::Int(v) => v.to_string(),
        SqlValue::Bool(v) => if *v { "true" } else { "false" }.into(),
        SqlValue::Text(v) => quote(v),
    }
}

/// Substitutes `$n` with literals taken from already-parsed SQL values.
///
/// This is the same scanner the extended protocol uses, so `EXECUTE p(1)` and a
/// `Bind` of the same statement produce identical SQL text. The caller checks the
/// argument count; the check here is a guard, not the user-facing error.
fn substitute_values(sql: &str, values: &[SqlValue]) -> Result<String, PgError> {
    let scan = scan(sql)?;
    let required = scan.placeholders.iter().map(|p| p.index).max().unwrap_or(0);
    if values.len() as u32 != required {
        return Err(PgError::new(
            "08P01",
            format!(
                "bind message supplies {} parameters, but prepared statement requires {required}",
                values.len()
            ),
        ));
    }
    let mut out = String::with_capacity(sql.len());
    let mut last = 0;
    for p in &scan.placeholders {
        out.push_str(&sql[last..p.start]);
        out.push_str(&literal_sql(&values[p.index as usize - 1]));
        last = p.end;
    }
    out.push_str(&sql[last..]);
    Ok(out)
}

/// Cursor over one statement's text, used only to read the three session
/// commands (`PREPARE`, `EXECUTE`, `DEALLOCATE`). It is deliberately not a
/// general SQL parser: anything it does not understand is reported, never guessed.
struct Words<'a> {
    text: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Words<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            bytes: text.as_bytes(),
            pos: 0,
        }
    }

    /// Skips whitespace, `--` line comments and nested `/* */` block comments.
    /// A statement read from a file usually starts with one of these.
    fn skip_ws(&mut self) {
        loop {
            while self
                .bytes
                .get(self.pos)
                .is_some_and(|b| b.is_ascii_whitespace())
            {
                self.pos += 1;
            }
            if self.bytes[self.pos..].starts_with(b"--") {
                while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else if self.bytes[self.pos..].starts_with(b"/*") {
                let mut depth = 1u32;
                self.pos += 2;
                while depth > 0 {
                    if self.bytes[self.pos..].starts_with(b"/*") {
                        depth += 1;
                        self.pos += 2;
                    } else if self.bytes[self.pos..].starts_with(b"*/") {
                        depth -= 1;
                        self.pos += 2;
                    } else if self.pos < self.bytes.len() {
                        self.pos += 1;
                    } else {
                        break; // unterminated; the parser reports it
                    }
                }
            } else {
                break;
            }
        }
    }

    fn peek_byte(&mut self) -> Option<u8> {
        self.skip_ws();
        self.bytes.get(self.pos).copied()
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.peek_byte() == Some(byte) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Reads one unquoted identifier.
    fn word(&mut self) -> Option<&'a str> {
        self.skip_ws();
        let start = self.pos;
        match self.bytes.get(self.pos) {
            Some(b) if b.is_ascii_alphabetic() || *b == b'_' => self.pos += 1,
            _ => return None,
        }
        while let Some(b) = self.bytes.get(self.pos) {
            if b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$' {
                self.pos += 1;
            } else {
                break;
            }
        }
        Some(&self.text[start..self.pos])
    }

    fn peek_word(&mut self) -> Option<&'a str> {
        let saved = self.pos;
        let word = self.word();
        self.pos = saved;
        word
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        match self.peek_word() {
            Some(word) if word.eq_ignore_ascii_case(kw) => {
                self.word();
                true
            }
            _ => false,
        }
    }

    fn rest(&mut self) -> &'a str {
        self.skip_ws();
        &self.text[self.pos..]
    }

    fn at_end(&mut self) -> bool {
        self.rest().is_empty()
    }

    /// Reads the body of a parenthesised group after its `(` has been
    /// consumed, up to and including the matching `)`, and returns the text
    /// between them. Used by `COPY (<query>) TO STDOUT`.
    fn group(&mut self) -> Result<&'a str, PgError> {
        self.skip_ws();
        let start = self.pos;
        let mut depth = 0i32;
        while let Some(&b) = self.bytes.get(self.pos) {
            match b {
                b'\'' => {
                    self.pos += 1;
                    loop {
                        match self.bytes.get(self.pos) {
                            None => {
                                return Err(PgError::new("42601", "unterminated string literal"))
                            }
                            Some(b'\'') if self.bytes.get(self.pos + 1) == Some(&b'\'') => {
                                self.pos += 2;
                            }
                            Some(b'\'') => {
                                self.pos += 1;
                                break;
                            }
                            Some(_) => self.pos += 1,
                        }
                    }
                }
                b'"' => {
                    self.pos += 1;
                    loop {
                        match self.bytes.get(self.pos) {
                            None => {
                                return Err(PgError::new("42601", "unterminated quoted identifier"))
                            }
                            Some(b'"') if self.bytes.get(self.pos + 1) == Some(&b'"') => {
                                self.pos += 2;
                            }
                            Some(b'"') => {
                                self.pos += 1;
                                break;
                            }
                            Some(_) => self.pos += 1,
                        }
                    }
                }
                b'(' => {
                    depth += 1;
                    self.pos += 1;
                }
                b')' => {
                    if depth == 0 {
                        let inner = &self.text[start..self.pos];
                        self.pos += 1;
                        return Ok(inner.trim());
                    }
                    depth -= 1;
                    self.pos += 1;
                }
                _ => self.pos += 1,
            }
        }
        Err(PgError::new("42601", "unterminated parenthesis in COPY"))
    }

    /// Reads one comma/parenthesis-delimited argument, quote aware.
    fn arg(&mut self) -> Result<&'a str, PgError> {
        self.skip_ws();
        let start = self.pos;
        let mut depth = 0i32;
        let mut quoted = false;
        while let Some(&b) = self.bytes.get(self.pos) {
            if quoted {
                if b == b'\'' {
                    if self.bytes.get(self.pos + 1) == Some(&b'\'') {
                        self.pos += 1;
                    } else {
                        quoted = false;
                    }
                }
            } else {
                match b {
                    b'\'' => quoted = true,
                    b'(' => depth += 1,
                    b')' | b',' if depth == 0 => break,
                    _ => {}
                }
            }
            self.pos += 1;
        }
        if quoted {
            return Err(PgError::new("42601", "unterminated string literal"));
        }
        let arg = self.text[start..self.pos].trim();
        if arg.is_empty() {
            return Err(PgError::new("42601", "empty argument"));
        }
        Ok(arg)
    }
}

/// The session statements the pgwire layer executes itself.
#[derive(Debug)]
enum SessionCmd {
    Prepare {
        name: String,
        oids: Vec<u32>,
        sql: String,
    },
    Execute {
        name: String,
        args: Vec<SqlValue>,
    },
    Deallocate {
        all: bool,
        name: String,
    },
    CopyIn(CopySpec),
    CopyOut(CopySpec),
}

/// One `COPY` statement. Only the client streams are accepted: a server-side
/// file would let a database user read and write daemon files.
#[derive(Debug)]
struct CopySpec {
    /// `COPY <table> [(columns)]`, or `None` for `COPY (<query>)`.
    table: Option<(String, Option<Vec<String>>)>,
    query: Option<String>,
}

/// The column types `COPY` needs to turn one text field into a typed literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyType {
    Int,
    Text,
    Bool,
}

#[derive(Debug, Clone)]
struct CopyColumn {
    name: String,
    kind: CopyType,
}

fn type_oid(name: &str) -> Result<u32, PgError> {
    Ok(match name {
        "bool" | "boolean" => 16,
        "int2" | "smallint" => 21,
        "int" | "int4" | "integer" => 23,
        "int8" | "bigint" => 20,
        "oid" => 26,
        "text" | "varchar" | "char" | "bpchar" | "name" | "character varying" => 25,
        "float4" | "real" => 700,
        "float8" | "double precision" => 701,
        other => {
            return Err(PgError::new(
                "42704",
                format!("type \"{other}\" does not exist"),
            ))
        }
    })
}

fn parse_prepare(w: &mut Words<'_>) -> Result<SessionCmd, PgError> {
    if w.eat_kw("TRANSACTION") {
        return Err(PgError::new(
            "0A000",
            "PREPARE TRANSACTION (two-phase commit) is not supported",
        ));
    }
    let name = w
        .word()
        .ok_or_else(|| PgError::new("42601", "PREPARE requires a statement name"))?;
    if name.len() > MAX_NAME {
        return Err(PgError::new("54000", "prepared statement name is too long"));
    }
    let mut oids = Vec::new();
    if w.eat(b'(') {
        loop {
            let mut ty = w
                .word()
                .ok_or_else(|| PgError::new("42601", "PREPARE parameter type list is malformed"))?
                .to_ascii_lowercase();
            if ty == "double" && w.eat_kw("precision") {
                ty = "double precision".into();
            } else if ty == "character" && w.eat_kw("varying") {
                ty = "character varying".into();
            }
            oids.push(type_oid(&ty)?);
            if w.eat(b',') {
                continue;
            }
            if w.eat(b')') {
                break;
            }
            return Err(PgError::new(
                "42601",
                "PREPARE parameter type list is malformed",
            ));
        }
    }
    if !w.eat_kw("AS") {
        return Err(PgError::new("42601", "PREPARE requires AS <statement>"));
    }
    let body = w.rest().trim();
    if body.is_empty() {
        return Err(PgError::new(
            "42601",
            "PREPARE requires a statement after AS",
        ));
    }
    Ok(SessionCmd::Prepare {
        name: name.to_string(),
        oids,
        sql: body.to_string(),
    })
}

fn parse_execute(w: &mut Words<'_>) -> Result<SessionCmd, PgError> {
    let name = w
        .word()
        .ok_or_else(|| PgError::new("42601", "EXECUTE requires a statement name"))?;
    let mut args = Vec::new();
    if w.eat(b'(') {
        loop {
            if w.eat(b')') {
                break;
            }
            let arg = w.arg()?;
            args.push(parse_sql_value(arg).map_err(|e| {
                PgError::new(
                    "0A000",
                    format!("EXECUTE arguments must be SQL literals: {e}"),
                )
            })?);
            if w.eat(b',') {
                continue;
            }
            if w.eat(b')') {
                break;
            }
            return Err(PgError::new("42601", "EXECUTE argument list is malformed"));
        }
    }
    if !w.at_end() {
        return Err(PgError::new(
            "42601",
            "unexpected text after EXECUTE arguments",
        ));
    }
    Ok(SessionCmd::Execute {
        name: name.to_string(),
        args,
    })
}

fn parse_deallocate(w: &mut Words<'_>) -> Result<SessionCmd, PgError> {
    let _ = w.eat_kw("PREPARE");
    if w.eat_kw("ALL") {
        if !w.at_end() {
            return Err(PgError::new(
                "42601",
                "unexpected text after DEALLOCATE ALL",
            ));
        }
        return Ok(SessionCmd::Deallocate {
            all: true,
            name: String::new(),
        });
    }
    let name = w
        .word()
        .ok_or_else(|| PgError::new("42601", "DEALLOCATE requires a statement name or ALL"))?;
    if !w.at_end() {
        return Err(PgError::new("42601", "unexpected text after DEALLOCATE"));
    }
    Ok(SessionCmd::Deallocate {
        all: false,
        name: name.to_string(),
    })
}

/// Recognises the three statements the legacy SQL parser cannot represent
/// (`PREPARE`/`EXECUTE`/`DEALLOCATE` carry `$n` and a nested statement).
/// Returns `Ok(None)` for everything else, which then takes the normal path.
fn session_cmd(text: &str) -> Result<Option<SessionCmd>, PgError> {
    let mut w = Words::new(text);
    let Some(first) = w.peek_word() else {
        return Ok(None);
    };
    if first.eq_ignore_ascii_case("PREPARE") {
        w.word();
        return parse_prepare(&mut w).map(Some);
    }
    if first.eq_ignore_ascii_case("EXECUTE") {
        w.word();
        return parse_execute(&mut w).map(Some);
    }
    if first.eq_ignore_ascii_case("DEALLOCATE") {
        w.word();
        return parse_deallocate(&mut w).map(Some);
    }
    if first.eq_ignore_ascii_case("COPY") {
        w.word();
        return parse_copy(&mut w).map(Some);
    }
    Ok(None)
}

/// Parses `COPY <table> [(columns)] {FROM STDIN | TO STDOUT}` and
/// `COPY (<query>) TO STDOUT`, in text format with the default settings.
fn parse_copy(w: &mut Words<'_>) -> Result<SessionCmd, PgError> {
    let (table, query) = if w.eat(b'(') {
        (None, Some(w.group()?.to_string()))
    } else {
        let table = w
            .word()
            .ok_or_else(|| PgError::new("42601", "COPY requires a table name"))?;
        if table.len() > MAX_NAME {
            return Err(PgError::new("54000", "table name is too long"));
        }
        let mut columns = None;
        if w.eat(b'(') {
            let mut cols = Vec::new();
            loop {
                let name = w
                    .word()
                    .ok_or_else(|| PgError::new("42601", "malformed COPY column list"))?;
                if name.len() > MAX_NAME {
                    return Err(PgError::new("54000", "column name is too long"));
                }
                cols.push(name.to_string());
                if w.eat(b',') {
                    continue;
                }
                if w.eat(b')') {
                    break;
                }
                return Err(PgError::new("42601", "malformed COPY column list"));
            }
            if cols.is_empty() {
                return Err(PgError::new("42601", "COPY column list is empty"));
            }
            columns = Some(cols);
        }
        (Some((table.to_string(), columns)), None)
    };
    let from = if w.eat_kw("FROM") {
        true
    } else if w.eat_kw("TO") {
        false
    } else {
        return Err(PgError::new(
            "42601",
            "COPY requires FROM STDIN or TO STDOUT",
        ));
    };
    if w.peek_byte() == Some(b'\'') {
        // `COPY ... FROM 'file'` reads a daemon file as the database user.
        return Err(PgError::new(
            "0A000",
            "server-side COPY files are not supported; use STDIN or STDOUT",
        ));
    }
    let stream = w
        .word()
        .ok_or_else(|| PgError::new("42601", "COPY requires STDIN or STDOUT"))?;
    if from && !stream.eq_ignore_ascii_case("STDIN") {
        return Err(PgError::new(
            "0A000",
            "server-side COPY FROM is not supported; use COPY ... FROM STDIN",
        ));
    }
    if !from && !stream.eq_ignore_ascii_case("STDOUT") {
        return Err(PgError::new(
            "0A000",
            "server-side COPY TO is not supported; use COPY ... TO STDOUT",
        ));
    }
    if from && query.is_some() {
        return Err(PgError::new("42601", "COPY FROM requires a table"));
    }
    // Only the text format with default settings is implemented. Accepting an
    // option and ignoring it would silently change what the client's data
    // means, so every other spelling is refused.
    if !w.at_end() {
        let _ = w.eat_kw("WITH");
        if w.eat(b'(') {
            loop {
                let name = w
                    .word()
                    .ok_or_else(|| PgError::new("42601", "malformed COPY option list"))?;
                if !name.eq_ignore_ascii_case("format") {
                    return Err(PgError::new(
                        "0A000",
                        format!("COPY option \"{name}\" is not supported"),
                    ));
                }
                let value = w
                    .word()
                    .ok_or_else(|| PgError::new("42601", "COPY FORMAT requires a value"))?;
                if !value.eq_ignore_ascii_case("text") {
                    return Err(PgError::new(
                        "0A000",
                        "only the COPY text format is supported",
                    ));
                }
                if w.eat(b',') {
                    continue;
                }
                if w.eat(b')') {
                    break;
                }
                return Err(PgError::new("42601", "malformed COPY option list"));
            }
        } else if w.eat_kw("FORMAT") {
            let value = w
                .word()
                .ok_or_else(|| PgError::new("42601", "COPY FORMAT requires a value"))?;
            if !value.eq_ignore_ascii_case("text") {
                return Err(PgError::new(
                    "0A000",
                    "only the COPY text format is supported",
                ));
            }
        } else {
            return Err(PgError::new(
                "0A000",
                "COPY options are not supported except FORMAT text",
            ));
        }
        if !w.at_end() {
            return Err(PgError::new(
                "0A000",
                "COPY options are not supported except FORMAT text",
            ));
        }
    }
    let spec = CopySpec { table, query };
    Ok(if from {
        SessionCmd::CopyIn(spec)
    } else {
        SessionCmd::CopyOut(spec)
    })
}

/// One statement's outcome: rows, the command tag to report, and whether the
/// statement ended the transaction (which discards its portals).
#[derive(Debug)]
struct Completed {
    result: QueryResult,
    tag: String,
    ends_transaction: bool,
}

fn aborted_error() -> PgError {
    PgError::new("25P02", "transaction aborted; ROLLBACK required")
}

fn empty_result() -> QueryResult {
    QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected: 0,
    }
}

fn db_error(e: yadb_db::DbError) -> PgError {
    match e {
        yadb_db::DbError::UnrecognizedSetting(name) => PgError::new(
            "42704",
            format!("unrecognized configuration parameter \"{name}\""),
        ),
        other => PgError::new("42601", other.to_string()),
    }
}

/// The SQLSTATE for a non-200 answer from the gateway over the HTTP bridge.
///
/// The status alone is not enough. `503` is used both for "this node cannot
/// serve the request yet, try again" (`MetaError::StorageUnavailable`, a pool
/// that cannot reach redundancy, a stale control plane) and for "this write's
/// outcome could not be determined". The second must never be reported as a
/// retryable condition: the mutation may have committed, and the gateway marks
/// it with `x-yadb-retry: do-not-retry`. So the headers decide, and the
/// ambiguous case keeps `XX000`, which no driver treats as "retry".
///
/// `57P03` is the same code the session already uses for an unavailable
/// control plane, so a client sees one consistent "not ready" answer.
fn pg_sqlstate_for(code: u16, headers: &str) -> &'static str {
    match code {
        403 => "42501",
        503 => {
            let lower = headers.to_ascii_lowercase();
            if lower.contains("x-yadb-retry: do-not-retry")
                || lower.contains("x-yadb-write-outcome: unknown")
            {
                // Unknown outcome: not a syntax error and not a retry hint.
                "XX000"
            } else {
                "57P03"
            }
        }
        _ => "XX000",
    }
}

fn report_name(name: &str) -> &str {
    match name {
        "datestyle" => "DateStyle",
        "timezone" => "TimeZone",
        _ => name,
    }
}

/// Echoes a `GUC_REPORT` parameter change as a ParameterStatus message, which is
/// how drivers observe `SET client_encoding` and friends.
fn report_setting(c: &mut Stream, name: &str, value: &str) -> io::Result<()> {
    if matches!(
        name,
        "client_encoding"
            | "application_name"
            | "datestyle"
            | "timezone"
            | "standard_conforming_strings"
            | "integer_datetimes"
            | "server_version"
            | "extra_float_digits"
            | "is_superuser"
            | "session_authorization"
    ) {
        packet(
            c,
            b'S',
            format!("{}\0{}\0", report_name(name), value).as_bytes(),
        )?;
    }
    Ok(())
}

/// Statements the pgwire session owns: `SET`, `RESET`, `SHOW` and `SELECT`
/// without `FROM`. Returns `None` for everything the engine must execute.
fn local_stmt(
    c: &mut Stream,
    cx: &Conn,
    tx: &TxState,
    session: &mut Session,
    stmt: &SqlStmt,
) -> io::Result<Option<Result<Completed, PgError>>> {
    match stmt {
        SqlStmt::Set { name, value } => {
            if tx.status == b'E' {
                return Ok(Some(Err(aborted_error())));
            }
            match session.set(name, value) {
                Ok(()) => {
                    report_setting(c, name, value)?;
                    Ok(Some(Ok(Completed {
                        result: empty_result(),
                        tag: "SET".into(),
                        ends_transaction: false,
                    })))
                }
                Err(e) => Ok(Some(Err(e))),
            }
        }
        SqlStmt::SetDefault { name } => {
            if tx.status == b'E' {
                return Ok(Some(Err(aborted_error())));
            }
            session.reset(Some(&name));
            if let Some(value) = session.setting(&name, cx.subject) {
                report_setting(c, &name, &value)?;
            }
            Ok(Some(Ok(Completed {
                result: empty_result(),
                tag: "SET".into(),
                ends_transaction: false,
            })))
        }
        SqlStmt::Reset { name } => {
            if tx.status == b'E' {
                return Ok(Some(Err(aborted_error())));
            }
            session.reset(name.as_deref());
            if let Some(name) = name {
                if let Some(value) = session.setting(name, cx.subject) {
                    report_setting(c, name, &value)?;
                }
            }
            Ok(Some(Ok(Completed {
                result: empty_result(),
                tag: "RESET".into(),
                ends_transaction: false,
            })))
        }
        SqlStmt::ShowSetting { name } => {
            if tx.status == b'E' {
                return Ok(Some(Err(aborted_error())));
            }
            Ok(Some(match session.setting(name, cx.subject) {
                Some(value) => Ok(Completed {
                    result: QueryResult {
                        columns: vec![name.clone()],
                        rows: vec![vec![SqlValue::Text(value)]],
                        rows_affected: 0,
                    },
                    tag: "SHOW".into(),
                    ends_transaction: false,
                }),
                None => Err(PgError::new(
                    "42704",
                    format!("unrecognized configuration parameter \"{name}\""),
                )),
            }))
        }
        SqlStmt::ShowAll => Ok(Some(Err(PgError::new(
            "0A000",
            "SHOW ALL is not supported; SHOW <name> answers one parameter",
        )))),
        SqlStmt::SelectNoFrom { items } => {
            if tx.status == b'E' {
                return Ok(Some(Err(aborted_error())));
            }
            let scalar = ScalarContext {
                database: cx.database,
                user: cx.subject,
                settings: &session.settings,
            };
            Ok(Some(eval_scalar_select(items, &scalar).map_or_else(
                |e| Err(db_error(e)),
                |result| {
                    Ok(Completed {
                        tag: format!("SELECT {}", result.rows.len()),
                        result,
                        ends_transaction: false,
                    })
                },
            )))
        }
        _ => Ok(None),
    }
}

/// Runs one parsed statement: session statements locally, everything else
/// through the engine over HTTP.
fn run_stmt(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    stmt: &SqlStmt,
    sql: &str,
) -> io::Result<Result<Completed, PgError>> {
    if let Some(local) = local_stmt(c, cx, tx, session, stmt)? {
        return Ok(local);
    }
    let aborted_commit = tx.status == b'E' && matches!(stmt, SqlStmt::Commit);
    match exec_one(cx, tx, stmt, sql)? {
        Ok(result) => Ok(Ok(Completed {
            tag: tag(stmt, &result, aborted_commit),
            result,
            ends_transaction: matches!(stmt, SqlStmt::Commit | SqlStmt::Rollback),
        })),
        Err(e) => Ok(Err(e)),
    }
}

/// Parses and runs exactly one SQL statement (no `PREPARE`/`EXECUTE`/
/// `DEALLOCATE`, which only the statement text itself can introduce).
fn run_single_sql(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    sql: &str,
) -> io::Result<Result<Completed, PgError>> {
    let statements = match parse_sql_script(sql) {
        Ok(statements) => statements,
        Err(e) => return Ok(Err(PgError::new("42601", e.to_string()))),
    };
    if statements.len() != 1 {
        return Ok(Err(PgError::new(
            "42601",
            "cannot execute multiple commands in one statement",
        )));
    }
    run_stmt(c, cx, tx, session, &statements[0], sql)
}

/// One statement of a split script, classified before anything executes.
enum TextItem<'a> {
    Session(SessionCmd),
    /// A session command that is already known to be invalid; reporting it in
    /// order keeps `PREPARE TRANSACTION` and friends from being masked by the
    /// whole-script parse error.
    SessionError(PgError),
    Sql(SqlStmt, &'a str),
}

/// Classifies one split statement without running it.
///
/// `Err` means the text is neither a session command nor a single SQL
/// statement, so the caller must fall back to the original parse error.
fn plan_text(text: &str) -> Result<TextItem<'_>, PgError> {
    match session_cmd(text) {
        Ok(Some(cmd)) => Ok(TextItem::Session(cmd)),
        Ok(None) => match parse_sql_script(text) {
            Ok(mut statements) if statements.len() == 1 => {
                Ok(TextItem::Sql(statements.remove(0), text))
            }
            Ok(_) => Err(PgError::new("42601", "expected exactly one SQL statement")),
            Err(e) => Err(PgError::new("42601", e.to_string())),
        },
        Err(e) => Ok(TextItem::SessionError(e)),
    }
}

/// Runs one already split statement of client text.
fn run_text(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    text: &str,
) -> io::Result<Result<Completed, PgError>> {
    match plan_text(text) {
        Ok(TextItem::Session(cmd)) => exec_session_cmd(c, cx, tx, session, cmd),
        Ok(TextItem::SessionError(e)) => Ok(Err(e)),
        Ok(TextItem::Sql(stmt, sql)) => run_stmt(c, cx, tx, session, &stmt, sql),
        Err(e) => Ok(Err(e)),
    }
}

fn exec_session_cmd(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    cmd: SessionCmd,
) -> io::Result<Result<Completed, PgError>> {
    if tx.status == b'E' {
        return Ok(Err(aborted_error()));
    }
    match cmd {
        SessionCmd::Prepare { name, oids, sql } => Ok(prepare_sql(session, name, oids, sql)),
        SessionCmd::Execute { name, args } => exec_prepared(c, cx, tx, session, name, args),
        SessionCmd::Deallocate { all, name } => Ok(deallocate(session, all, name)),
        SessionCmd::CopyIn(spec) => copy_in(c, cx, tx, session, spec),
        SessionCmd::CopyOut(spec) => copy_out(c, cx, tx, session, spec),
    }
}

fn prepare_sql(
    session: &mut Session,
    name: String,
    oids: Vec<u32>,
    sql: String,
) -> Result<Completed, PgError> {
    if sql.len() > MAX_SQL {
        return Err(PgError::new("54000", "prepared statement SQL is too long"));
    }
    let scan = scan(&sql)?;
    if scan.statements == 0 {
        return Err(PgError::new("42601", "cannot prepare an empty query"));
    }
    if scan.statements > 1 {
        return Err(PgError::new(
            "42601",
            "cannot insert multiple commands into a prepared statement",
        ));
    }
    let params = scan.placeholders.iter().map(|p| p.index).max().unwrap_or(0);
    if oids.len() as u32 > params {
        return Err(PgError::new(
            "42P02",
            format!("there is no parameter ${}", params + 1),
        ));
    }
    if !session.statements.contains_key(&name) && session.statements.len() >= MAX_PREPARED {
        return Err(PgError::new(
            "54000",
            format!("at most {MAX_PREPARED} prepared statements are supported"),
        ));
    }
    if session.statements.contains_key(&name) {
        return Err(PgError::new(
            "42P05",
            format!("prepared statement \"{name}\" already exists"),
        ));
    }
    session
        .statements
        .insert(name, Prepared { sql, oids, params });
    Ok(Completed {
        result: empty_result(),
        tag: "PREPARE".into(),
        ends_transaction: false,
    })
}

/// Rejects an argument whose SQL literal cannot be the declared parameter type.
/// The engine has no float or numeric type, so those OIDs accept integers only.
fn check_arg_types(name: &str, args: &[SqlValue], oids: &[u32]) -> Result<(), PgError> {
    for (index, value) in args.iter().enumerate() {
        let oid = oids.get(index).copied().unwrap_or(0);
        let valid = match oid {
            16 => matches!(value, SqlValue::Bool(_) | SqlValue::Null),
            20 | 21 | 23 | 26 | 700 | 701 => matches!(value, SqlValue::Int(_) | SqlValue::Null),
            0 => true,
            _ => true,
        };
        if !valid {
            return Err(PgError::new(
                "22P02",
                format!(
                    "invalid input syntax for parameter ${} of prepared statement \"{name}\": {}",
                    index + 1,
                    literal_sql(value)
                ),
            ));
        }
    }
    Ok(())
}

fn exec_prepared(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    name: String,
    args: Vec<SqlValue>,
) -> io::Result<Result<Completed, PgError>> {
    let Some(prepared) = session.statements.get(&name) else {
        return Ok(Err(PgError::new(
            "26000",
            format!("prepared statement \"{name}\" does not exist"),
        )));
    };
    let sql = prepared.sql.clone();
    let params = prepared.params;
    let oids = prepared.oids.clone();
    if args.len() as u32 != params {
        return Ok(Err(PgError::new(
            "42601",
            format!(
                "wrong number of parameters for prepared statement \"{name}\": expected {params}, got {}",
                args.len()
            ),
        )));
    }
    if let Err(e) = check_arg_types(&name, &args, &oids) {
        return Ok(Err(e));
    }
    let sql = match substitute_values(&sql, &args) {
        Ok(sql) => sql,
        Err(e) => return Ok(Err(e)),
    };
    run_single_sql(c, cx, tx, session, &sql)
}

fn deallocate(session: &mut Session, all: bool, name: String) -> Result<Completed, PgError> {
    if all {
        session.statements.clear();
    } else if session.statements.remove(&name).is_none() {
        return Err(PgError::new(
            "26000",
            format!("prepared statement \"{name}\" does not exist"),
        ));
    }
    Ok(Completed {
        result: empty_result(),
        tag: "DEALLOCATE".into(),
        ends_transaction: false,
    })
}

#[derive(Clone)]
struct Prepared {
    sql: String,
    /// Parameter OIDs declared by the client, possibly shorter than `params`.
    oids: Vec<u32>,
    params: u32,
}
#[derive(Clone)]
struct Portal {
    /// Statement text with the bound literals already substituted.
    sql: String,
}

/// Immutable per-connection routing information shared by both query paths.
struct Conn<'a> {
    st: &'a AppState,
    project: &'a str,
    /// Database name (the part after `project/`), answered by `current_database()`.
    database: &'a str,
    path: &'a str,
    session: &'a str,
    subject: &'a str,
}
#[derive(Clone, Copy)]
struct TxState {
    status: u8,
    term: u64,
}

/// Per-connection state: configuration parameters, prepared statements and portals.
///
/// `PREPARE name AS ...` and the extended protocol's `Parse` share one namespace
/// (as they do in PostgreSQL's `pg_prepared_statements`), so `EXECUTE` runs a
/// statement the client parsed and `DEALLOCATE` removes whatever the name
/// currently refers to.
struct Session {
    settings: BTreeMap<String, String>,
    statements: BTreeMap<String, Prepared>,
    portals: BTreeMap<String, Portal>,
}

impl Session {
    /// Starts from the parameters the client sent in its startup packet. Values
    /// the client did not send are answered from `setting_default` instead of
    /// being stored, so there is exactly one default table.
    fn new(params: &BTreeMap<&str, &str>) -> Self {
        let mut settings = BTreeMap::new();
        for (key, value) in params {
            let key = key.to_ascii_lowercase();
            if matches!(
                key.as_str(),
                "user" | "database" | "replication" | "options" | "protocol_version"
            ) {
                continue;
            }
            settings.insert(key, (*value).to_string());
        }
        Self {
            settings,
            statements: BTreeMap::new(),
            portals: BTreeMap::new(),
        }
    }

    /// The value `SHOW <name>` answers with: the stored parameter, the
    /// documented default, or `None` when the parameter is not recognised.
    fn setting(&self, name: &str, user: &str) -> Option<String> {
        let name = name.to_ascii_lowercase();
        self.settings.get(&name).cloned().or_else(|| {
            if name == "session_authorization" {
                Some(user.to_string())
            } else {
                setting_default(&name).map(str::to_string)
            }
        })
    }

    /// Remembers a parameter verbatim. Unknown parameters are accepted on
    /// purpose - drivers set parameters this server does not implement - but a
    /// stored value never changes how any statement is executed.
    fn set(&mut self, name: &str, value: &str) -> Result<(), PgError> {
        if name.len() > MAX_NAME {
            return Err(PgError::new(
                "54000",
                "configuration parameter name is too long",
            ));
        }
        if !self.settings.contains_key(name) && self.settings.len() >= MAX_SETTINGS {
            return Err(PgError::new(
                "54000",
                format!("at most {MAX_SETTINGS} configuration parameters are supported"),
            ));
        }
        self.settings.insert(name.to_string(), value.to_string());
        Ok(())
    }

    fn reset(&mut self, name: Option<&str>) {
        match name {
            Some(name) => {
                self.settings.remove(&name.to_ascii_lowercase());
            }
            None => self.settings.clear(),
        }
    }
}

fn parse_ext(data: &[u8], statements: &mut BTreeMap<String, Prepared>) -> Result<(), PgError> {
    let mut r = Reader::new(data);
    let name = r.cstr()?.to_string();
    let sql = r.cstr()?.to_string();
    let declared = r.u16()? as usize;
    let mut oids = Vec::with_capacity(declared);
    for _ in 0..declared {
        oids.push(r.u32()?);
    }
    if name.len() > MAX_NAME {
        return Err(PgError::new("54000", "prepared statement name is too long"));
    }
    if sql.len() > MAX_SQL {
        return Err(PgError::new("54000", "prepared statement SQL is too long"));
    }
    let scan = scan(&sql)?;
    if scan.statements == 0 {
        return Err(PgError::new("42601", "cannot prepare an empty query"));
    }
    if scan.statements > 1 {
        return Err(PgError::new(
            "42601",
            "cannot insert multiple commands into a prepared statement",
        ));
    }
    let params = scan.placeholders.iter().map(|p| p.index).max().unwrap_or(0);
    if oids.len() as u32 > params {
        return Err(PgError::new(
            "42P02",
            format!("there is no parameter ${}", params + 1),
        ));
    }
    if !statements.contains_key(&name) && statements.len() >= MAX_PREPARED {
        return Err(PgError::new(
            "54000",
            format!("at most {MAX_PREPARED} prepared statements are supported"),
        ));
    }
    // PostgreSQL replaces the *unnamed* statement freely but refuses to parse a
    // second statement under a name that is already taken (`42P05`), because the
    // existing statement may be the target of a portal that is still open. Do the
    // same: a silent replacement would change what an already-bound portal runs.
    if !name.is_empty() && statements.contains_key(&name) {
        return Err(PgError::new(
            "42P05",
            format!("prepared statement \"{name}\" already exists"),
        ));
    }
    statements.insert(name, Prepared { sql, oids, params });
    Ok(())
}

fn bind_ext(
    data: &[u8],
    statements: &BTreeMap<String, Prepared>,
    portals: &mut BTreeMap<String, Portal>,
) -> Result<(), PgError> {
    let mut r = Reader::new(data);
    let portal = r.cstr()?.to_string();
    let statement = r.cstr()?.to_string();
    let formats = r.u16()? as usize;
    let mut format_codes = Vec::with_capacity(formats);
    for _ in 0..formats {
        format_codes.push(r.u16()?);
    }
    let count = r.u16()? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let len = r.i32()?;
        if len == -1 {
            values.push(None);
        } else if len < 0 {
            return Err(PgError::new("08P01", "invalid parameter length"));
        } else {
            let raw = r.take(len as usize)?;
            values.push(Some(std::str::from_utf8(raw).map_err(|_| {
                PgError::new("22021", "invalid UTF-8 in text parameter")
            })?));
        }
    }
    let results = r.u16()? as usize;
    let mut result_codes = Vec::with_capacity(results);
    for _ in 0..results {
        result_codes.push(r.u16()?);
    }
    if portal.len() > MAX_NAME {
        return Err(PgError::new("54000", "portal name is too long"));
    }
    if format_codes.iter().any(|code| *code != 0) {
        return Err(PgError::new(
            "0A000",
            "binary parameter format is not supported; use text format",
        ));
    }
    if formats > 1 && formats != count {
        return Err(PgError::new(
            "08P01",
            format!("bind message has {formats} parameter formats but {count} parameters"),
        ));
    }
    if result_codes.iter().any(|code| *code != 0) {
        return Err(PgError::new(
            "0A000",
            "binary result format is not supported; use text format",
        ));
    }
    let Some(prepared) = statements.get(&statement) else {
        return Err(PgError::new(
            "26000",
            format!("prepared statement \"{statement}\" does not exist"),
        ));
    };
    let sql = substitute(&prepared.sql, &values, &prepared.oids)?;
    if !portals.contains_key(&portal) && portals.len() >= MAX_PORTALS {
        return Err(PgError::new(
            "54000",
            format!("at most {MAX_PORTALS} portals are supported"),
        ));
    }
    portals.insert(portal, Portal { sql });
    Ok(())
}

fn close_ext(
    data: &[u8],
    statements: &mut BTreeMap<String, Prepared>,
    portals: &mut BTreeMap<String, Portal>,
) -> Result<(), PgError> {
    let mut r = Reader::new(data);
    let target = r.take(1)?[0];
    let name = r.cstr()?;
    match target {
        b'S' => {
            statements.remove(name);
        }
        b'P' => {
            portals.remove(name);
        }
        _ => return Err(PgError::new("08P01", "invalid Close target")),
    }
    // Closing a name that does not exist is not an error in PostgreSQL.
    Ok(())
}

/// `Describe` sends RowDescription only for read-only statements, because this engine
/// only knows column names after execution. Read-only statements are executed with
/// `NULL` for every parameter and their rows discarded; nothing mutating is executed.
fn describe_sql(
    c: &mut Stream,
    cx: &Conn,
    tx: &TxState,
    session: &Session,
    sql: &str,
) -> io::Result<()> {
    if tx.status == b'E' {
        return packet(c, b'n', &[]);
    }
    let Ok(statements) = parse_sql_script(sql) else {
        return packet(c, b'n', &[]);
    };
    if statements.len() != 1 {
        return packet(c, b'n', &[]);
    }
    match &statements[0] {
        // Column names for these are known locally, without touching the engine.
        SqlStmt::SelectNoFrom { items } => {
            let scalar = ScalarContext {
                database: cx.database,
                user: cx.subject,
                settings: &session.settings,
            };
            return match eval_scalar_select(items, &scalar) {
                Ok(result) => packet(c, b'T', &row_description(&result.columns)),
                Err(_) => packet(c, b'n', &[]),
            };
        }
        SqlStmt::ShowSetting { name } => {
            return packet(c, b'T', &row_description(std::slice::from_ref(name)));
        }
        SqlStmt::Select { .. } | SqlStmt::ShowTables | SqlStmt::ShowIndexes { .. } => {}
        _ => return packet(c, b'n', &[]),
    }
    // Probe outside the client's transaction session: a failed statement inside that
    // session would discard the server-side transaction buffer, and column metadata
    // does not need the transaction's rows or snapshot.
    let body = serde_json::to_vec(&serde_json::json!({
        "sql": sql,
        "session_id": format!("{}-describe", cx.session),
        "force_leader": true,
    }))
    .map_err(io::Error::other)?;
    let (code, body) = http(
        &cx.st.listen_addr,
        "POST",
        cx.path,
        cx.subject,
        cx.st.subject_secret.as_deref(),
        &body,
    )?;
    let Ok(result) = serde_json::from_slice::<QueryResult>(&body) else {
        return packet(c, b'n', &[]);
    };
    if code != 200 || result.columns.is_empty() {
        return packet(c, b'n', &[]);
    }
    packet(c, b'T', &row_description(&result.columns))
}

fn describe_ext(
    c: &mut Stream,
    cx: &Conn,
    tx: &TxState,
    session: &Session,
    data: &[u8],
) -> io::Result<Result<(), PgError>> {
    let mut r = Reader::new(data);
    let target = match r.take(1) {
        Ok(v) => v[0],
        Err(e) => return Ok(Err(e)),
    };
    let name = match r.cstr() {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    match target {
        b'S' => {
            let Some(prepared) = session.statements.get(name) else {
                return Ok(Err(PgError::new(
                    "26000",
                    format!("prepared statement \"{name}\" does not exist"),
                )));
            };
            let sql = match substitute_nulls(&prepared.sql) {
                Ok(sql) => sql,
                Err(e) => return Ok(Err(e)),
            };
            let mut desc = Vec::with_capacity(2 + 4 * prepared.params as usize);
            desc.extend_from_slice(&(prepared.params as i16).to_be_bytes());
            for index in 0..prepared.params as usize {
                let oid = prepared.oids.get(index).copied().unwrap_or(0);
                desc.extend_from_slice(&oid.to_be_bytes());
            }
            packet(c, b't', &desc)?;
            describe_sql(c, cx, tx, session, &sql)?;
        }
        b'P' => {
            let Some(portal) = session.portals.get(name) else {
                return Ok(Err(PgError::new(
                    "34000",
                    format!("portal \"{name}\" does not exist"),
                )));
            };
            describe_sql(c, cx, tx, session, &portal.sql)?;
        }
        _ => return Ok(Err(PgError::new("08P01", "invalid Describe target"))),
    }
    Ok(Ok(()))
}

fn execute_ext(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    data: &[u8],
) -> io::Result<Result<(), PgError>> {
    let mut r = Reader::new(data);
    let name = match r.cstr() {
        Ok(name) => name,
        Err(e) => return Ok(Err(e)),
    };
    let max_rows = match r.i32() {
        Ok(v) => v,
        Err(e) => return Ok(Err(e)),
    };
    if max_rows != 0 {
        return Ok(Err(PgError::new(
            "0A000",
            "Execute row limits (PortalSuspended) are not supported; use max_rows=0",
        )));
    }
    let sql = match session.portals.get(name) {
        Some(portal) => portal.sql.clone(),
        None => {
            return Ok(Err(PgError::new(
                "34000",
                format!("portal \"{name}\" does not exist"),
            )))
        }
    };
    let completed = match run_text(c, cx, tx, session, &sql)? {
        Ok(completed) => completed,
        Err(e) => return Ok(Err(e)),
    };
    data_rows(c, &completed.result)?;
    packet(c, b'C', format!("{}\0", completed.tag).as_bytes())?;
    // Portals do not outlive the transaction that created them.
    if completed.ends_transaction {
        session.portals.clear();
    }
    Ok(Ok(()))
}

/// Dispatch one extended-protocol message. Returns false when an ErrorResponse was
/// sent and the caller must ignore messages until Sync.
fn extended(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    kind: u8,
    data: &[u8],
) -> io::Result<bool> {
    let result = match kind {
        b'P' => parse_ext(data, &mut session.statements),
        b'B' => bind_ext(data, &session.statements, &mut session.portals),
        b'C' => close_ext(data, &mut session.statements, &mut session.portals),
        b'D' => describe_ext(c, cx, tx, session, data)?,
        b'E' => execute_ext(c, cx, tx, session, data)?,
        _ => return Ok(false),
    };
    match result {
        Ok(()) => {
            match kind {
                b'P' => packet(c, b'1', &[])?,
                b'B' => packet(c, b'2', &[])?,
                b'C' => packet(c, b'3', &[])?,
                _ => {}
            }
            Ok(true)
        }
        Err(e) => {
            error(c, e.code, &e.message)?;
            Ok(false)
        }
    }
}

/// Run one parsed statement through the HTTP gateway, applying the shared transaction,
/// abort and leader-term rules. Returns the result or the error that must be reported.
fn exec_one(
    cx: &Conn,
    tx: &mut TxState,
    stmt: &SqlStmt,
    sql: &str,
) -> io::Result<Result<QueryResult, PgError>> {
    let aborted_commit = tx.status == b'E' && matches!(stmt, SqlStmt::Commit);
    let rollback = matches!(stmt, SqlStmt::Rollback) || aborted_commit;
    if tx.status == b'E' && !rollback {
        return Ok(Err(PgError::new(
            "25P02",
            "transaction aborted; ROLLBACK required",
        )));
    }
    let term = if cx.st.meta.is_openraft() {
        let id = cx
            .st
            .control
            .read()
            .unwrap()
            .projects
            .get(cx.project)
            .map(|(id, _)| *id)
            .ok_or_else(|| io::Error::other("project unavailable"))?;
        cx.st
            .meta
            .role_info_for_key(&format!("/db/{id}/"))
            .map_err(io::Error::other)?
            .2
    } else {
        cx.st.meta.role_info().2
    };
    if tx.status == b'T' && tx.term != term && !rollback {
        tx.status = b'E';
        return Ok(Err(PgError::new(
            "40001",
            "leader term changed; transaction must be restarted",
        )));
    }
    let sql = if aborted_commit { "ROLLBACK;" } else { sql };
    let body = serde_json::to_vec(&serde_json::json!({
        "sql": sql,
        "session_id": cx.session,
        "force_leader": true,
        "require_transaction": tx.status == b'T' && !rollback,
    }))
    .map_err(io::Error::other)?;
    let (code, headers, body) = http_with_headers(
        &cx.st.listen_addr,
        "POST",
        cx.path,
        cx.subject,
        cx.st.subject_secret.as_deref(),
        &body,
    )?;
    if code != 200 {
        if tx.status == b'T' {
            tx.status = b'E';
        }
        return Ok(Err(PgError::new(
            pg_sqlstate_for(code, &headers),
            String::from_utf8_lossy(&body),
        )));
    }
    let result: QueryResult = serde_json::from_slice(&body).map_err(io::Error::other)?;
    match stmt {
        SqlStmt::Begin => {
            tx.status = b'T';
            tx.term = term;
        }
        SqlStmt::Commit | SqlStmt::Rollback => tx.status = b'I',
        _ => {}
    }
    Ok(Ok(result))
}

fn tag(stmt: &SqlStmt, res: &QueryResult, aborted_commit: bool) -> String {
    match stmt {
        SqlStmt::Begin => "BEGIN".into(),
        SqlStmt::Commit => if aborted_commit { "ROLLBACK" } else { "COMMIT" }.into(),
        SqlStmt::Rollback => "ROLLBACK".into(),
        SqlStmt::Select { .. } => format!("SELECT {}", res.rows.len()),
        SqlStmt::Insert { .. } => format!("INSERT 0 {}", res.rows_affected),
        SqlStmt::Update { .. } => format!("UPDATE {}", res.rows_affected),
        SqlStmt::Delete { .. } => format!("DELETE {}", res.rows_affected),
        SqlStmt::CreateTable { .. } => "CREATE TABLE".into(),
        SqlStmt::CreateIndex { .. } => "CREATE INDEX".into(),
        SqlStmt::DropIndex { .. } => "DROP INDEX".into(),
        SqlStmt::SelectNoFrom { .. } => format!("SELECT {}", res.rows.len()),
        SqlStmt::Set { .. } => "SET".into(),
        SqlStmt::SetDefault { .. } => "SET".into(),
        SqlStmt::Reset { .. } => "RESET".into(),
        SqlStmt::ShowSetting { .. } | SqlStmt::ShowAll => "SHOW".into(),
        _ => "SHOW".into(),
    }
}

/// Column metadata for one `COPY` target. The engine is the only authority on
/// column types, so the target table is described with `SHOW COLUMNS` before the
/// text stream is typed; guessing from the values would silently corrupt a text
/// column that happens to look numeric.
fn copy_schema(cx: &Conn, table: &str) -> Result<Vec<CopyColumn>, PgError> {
    let body = serde_json::to_vec(&serde_json::json!({
        "sql": format!("SHOW COLUMNS FROM {table}"),
        "force_leader": true,
    }))
    .map_err(|e| PgError::new("XX000", e.to_string()))?;
    let (code, body) = http(
        &cx.st.listen_addr,
        "POST",
        cx.path,
        cx.subject,
        cx.st.subject_secret.as_deref(),
        &body,
    )
    .map_err(|e| PgError::new("08006", e.to_string()))?;
    if code != 200 {
        return Err(PgError::new(
            if code == 404 { "42P01" } else { "XX000" },
            String::from_utf8_lossy(&body),
        ));
    }
    let result: QueryResult =
        serde_json::from_slice(&body).map_err(|e| PgError::new("XX000", e.to_string()))?;
    let mut columns = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        let (Some(SqlValue::Text(name)), Some(SqlValue::Text(kind))) = (row.first(), row.get(1))
        else {
            return Err(PgError::new("XX000", "malformed column metadata"));
        };
        let kind = match kind.as_str() {
            "int" => CopyType::Int,
            "text" => CopyType::Text,
            "bool" => CopyType::Bool,
            other => {
                return Err(PgError::new(
                    "XX000",
                    format!("unknown column type \"{other}\""),
                ))
            }
        };
        columns.push(CopyColumn {
            name: name.clone(),
            kind,
        });
    }
    Ok(columns)
}

/// Unescapes one `COPY` text-format field. The result is bytes because an octal
/// or hex escape can produce any octet; the caller validates UTF-8.
fn copy_unescape(raw: &[u8]) -> Result<Vec<u8>, PgError> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] != b'\\' {
            out.push(raw[i]);
            i += 1;
            continue;
        }
        i += 1;
        let Some(&next) = raw.get(i) else {
            return Err(PgError::new("22P04", "unterminated COPY escape sequence"));
        };
        i += 1;
        match next {
            b'b' => out.push(8),
            b'f' => out.push(12),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(11),
            b'\\' => out.push(b'\\'),
            b'.' => out.push(b'.'),
            b'x' => {
                let mut value: u32 = 0;
                let mut digits = 0;
                while digits < 2 {
                    match raw.get(i).and_then(|c| (*c as char).to_digit(16)) {
                        Some(digit) => {
                            value = value * 16 + digit;
                            i += 1;
                            digits += 1;
                        }
                        None => break,
                    }
                }
                if digits == 0 {
                    return Err(PgError::new("22P04", "invalid COPY hex escape"));
                }
                out.push(value as u8);
            }
            b'0'..=b'7' => {
                let mut value = u32::from(next - b'0');
                let mut digits = 1;
                while digits < 3 {
                    match raw.get(i) {
                        Some(c @ b'0'..=b'7') => {
                            value = value * 8 + u32::from(c - b'0');
                            i += 1;
                            digits += 1;
                        }
                        _ => break,
                    }
                }
                if value > 255 {
                    return Err(PgError::new("22P04", "invalid COPY octal escape"));
                }
                out.push(value as u8);
            }
            other => out.push(other),
        }
    }
    Ok(out)
}

/// Turns one raw field into the SQL literal an ordinary `INSERT` would carry.
fn copy_literal(raw: &[u8], kind: CopyType) -> Result<String, PgError> {
    if raw == b"\\N" {
        return Ok("NULL".into());
    }
    let value = String::from_utf8(copy_unescape(raw)?)
        .map_err(|_| PgError::new("22021", "invalid byte sequence for encoding \"UTF8\""))?;
    match kind {
        CopyType::Text => Ok(quote(&value)),
        CopyType::Int => value.parse::<i64>().map(|v| v.to_string()).map_err(|_| {
            PgError::new(
                "22P02",
                format!("invalid input syntax for type integer: \"{value}\""),
            )
        }),
        CopyType::Bool => bool_literal(&value),
    }
}

fn copy_row(line: &[u8], columns: &[CopyColumn]) -> Result<Vec<String>, PgError> {
    let fields: Vec<&[u8]> = line.split(|b| *b == b'\t').collect();
    if fields.len() < columns.len() {
        return Err(PgError::new(
            "22P04",
            format!("missing data for column \"{}\"", columns[fields.len()].name),
        ));
    }
    if fields.len() > columns.len() {
        return Err(PgError::new(
            "22P04",
            format!(
                "extra data after last expected column (expected {}, got {})",
                columns.len(),
                fields.len()
            ),
        ));
    }
    fields
        .iter()
        .zip(columns)
        .map(|(raw, column)| copy_literal(raw, column.kind))
        .collect()
}

fn copy_insert(table: &str, columns: &[CopyColumn], batch: &[Vec<String>]) -> String {
    let mut sql = String::from("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (");
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str(&column.name);
    }
    sql.push_str(") VALUES ");
    for (row_index, row) in batch.iter().enumerate() {
        if row_index > 0 {
            sql.push(',');
        }
        sql.push('(');
        for (index, value) in row.iter().enumerate() {
            if index > 0 {
                sql.push(',');
            }
            sql.push_str(value);
        }
        sql.push(')');
    }
    sql
}

fn copy_flush(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    table: &str,
    columns: &[CopyColumn],
    batch: &mut Vec<Vec<String>>,
) -> io::Result<Result<(), PgError>> {
    let sql = copy_insert(table, columns, batch);
    batch.clear();
    match run_text(c, cx, tx, session, &sql)? {
        Ok(_) => Ok(Ok(())),
        Err(e) => Ok(Err(e)),
    }
}

/// Ends the implicit COPY transaction after a failure.
///
/// A rollback that cannot be confirmed leaves a transaction open that the
/// client cannot see, so the connection is closed instead of being reported as
/// clean and idle. The original COPY error is returned only when cleanup is
/// known to have happened.
fn copy_rollback(
    c: &mut Stream,
    cx: &Conn,
    work: &mut TxState,
    session: &mut Session,
) -> io::Result<()> {
    match run_text(c, cx, work, session, "ROLLBACK") {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(io::Error::other(format!(
            "COPY cleanup failed: {}: {}",
            e.code, e.message
        ))),
        Err(e) => Err(e),
    }
}

/// Removes one text-format line terminator from a drained line. A CRLF pair is
/// one terminator; a lone LF keeps its old meaning. The CR is not row data.
fn copy_strip_eol(line: &mut Vec<u8>) {
    line.pop();
    if line.last() == Some(&b'\r') {
        line.pop();
    }
}

fn copy_in(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    spec: CopySpec,
) -> io::Result<Result<Completed, PgError>> {
    if tx.status == b'E' {
        return Ok(Err(aborted_error()));
    }
    let Some((table, listed)) = spec.table else {
        return Ok(Err(PgError::new("42601", "COPY FROM requires a table")));
    };
    let schema = match copy_schema(cx, &table) {
        Ok(schema) => schema,
        Err(e) => return Ok(Err(e)),
    };
    let mut columns: Vec<CopyColumn> = Vec::new();
    match &listed {
        None => columns.extend(schema.iter().cloned()),
        Some(names) => {
            for name in names {
                let Some(column) = schema.iter().find(|c| c.name.eq_ignore_ascii_case(name)) else {
                    return Ok(Err(PgError::new(
                        "42703",
                        format!("column \"{name}\" of relation \"{table}\" does not exist"),
                    )));
                };
                if columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(&column.name))
                {
                    return Ok(Err(PgError::new(
                        "42701",
                        format!("column \"{name}\" specified more than once"),
                    )));
                }
                columns.push(column.clone());
            }
        }
    }
    // CopyInResponse: text format, one format code per column.
    let mut body = vec![0u8];
    body.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for _ in &columns {
        body.extend_from_slice(&0u16.to_be_bytes());
    }
    packet(c, b'G', &body)?;
    c.flush()?;

    // Outside an explicit transaction the whole COPY is one transaction, so a
    // failure halfway cannot leave a partial load behind. Inside one, the
    // client's transaction already provides that.
    let implicit_tx = tx.status != b'T';
    let mut implicit = if implicit_tx {
        Some(TxState {
            status: b'I',
            term: 0,
        })
    } else {
        None
    };
    let work = implicit.as_mut().unwrap_or(tx);
    if implicit_tx {
        match run_text(c, cx, work, session, "BEGIN")? {
            Ok(_) => {}
            Err(e) => return Ok(Err(e)),
        }
    }

    let mut pending: Vec<u8> = Vec::new();
    let mut batch: Vec<Vec<String>> = Vec::new();
    let mut batch_bytes = 0usize;
    let mut rows = 0u64;
    let mut failed: Option<PgError> = None;
    let mut end_of_data = false;
    loop {
        let mut kind = [0];
        if let Err(e) = c.read_exact(&mut kind) {
            // A client that disappears mid-COPY must not leave its rows behind.
            if implicit_tx {
                copy_rollback(c, cx, work, session)?;
            }
            return Err(e);
        }
        let data = frame(c)?;
        match kind[0] {
            b'd' if !end_of_data => {
                pending.extend_from_slice(&data);
                while let Some(position) = pending.iter().position(|b| *b == b'\n') {
                    let mut line: Vec<u8> = pending.drain(..=position).collect();
                    copy_strip_eol(&mut line);
                    if line.len() > MAX_COPY_LINE {
                        failed = Some(PgError::new("54000", "COPY line is too long"));
                        pending.clear();
                        end_of_data = true;
                        break;
                    }
                    if line == b"\\." {
                        // The text format's explicit end-of-data marker.
                        end_of_data = true;
                        pending.clear();
                        break;
                    }
                    if failed.is_some() {
                        continue;
                    }
                    match copy_row(&line, &columns) {
                        Ok(values) => {
                            batch_bytes += line.len() + 3;
                            batch.push(values);
                            rows += 1;
                            if batch.len() >= COPY_BATCH_ROWS || batch_bytes >= COPY_BATCH_BYTES {
                                match copy_flush(
                                    c, cx, work, session, &table, &columns, &mut batch,
                                )? {
                                    Ok(()) => {}
                                    Err(e) => failed = Some(e),
                                }
                                batch_bytes = 0;
                            }
                        }
                        Err(e) => {
                            failed = Some(e);
                            batch.clear();
                        }
                    }
                }
                // The limit is per line, not per frame: a frame carrying many
                // short rows is legal however large it is. Only the unterminated
                // remainder is bounded here.
                if !end_of_data && pending.len() > MAX_COPY_LINE {
                    if failed.is_none() {
                        failed = Some(PgError::new("54000", "COPY line is too long"));
                    }
                    pending.clear();
                    end_of_data = true;
                }
            }
            b'd' => {}
            b'c' => break,
            b'f' => {
                let message = cstr(&data).unwrap_or("client aborted");
                if implicit_tx {
                    copy_rollback(c, cx, work, session)?;
                }
                return Ok(Err(PgError::new(
                    "57014",
                    format!("COPY from stdin failed: {message}"),
                )));
            }
            b'H' => {}
            b'X' => {
                if implicit_tx {
                    copy_rollback(c, cx, work, session)?;
                }
                return Err(io::Error::other("client terminated during COPY"));
            }
            _ => {
                if implicit_tx {
                    copy_rollback(c, cx, work, session)?;
                }
                return Ok(Err(PgError::new("08P01", "unexpected message during COPY")));
            }
        }
    }
    // A trailing line without its newline is still a row.
    if failed.is_none() && !end_of_data && !pending.is_empty() {
        let mut line = std::mem::take(&mut pending);
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        if line.len() > MAX_COPY_LINE {
            failed = Some(PgError::new("54000", "COPY line is too long"));
        } else {
            match copy_row(&line, &columns) {
                Ok(values) => {
                    batch.push(values);
                    rows += 1;
                }
                Err(e) => failed = Some(e),
            }
        }
    }
    if failed.is_none() && !batch.is_empty() {
        match copy_flush(c, cx, work, session, &table, &columns, &mut batch)? {
            Ok(()) => {}
            Err(e) => failed = Some(e),
        }
    }
    match failed {
        Some(e) => {
            if implicit_tx {
                copy_rollback(c, cx, work, session)?;
            }
            Ok(Err(e))
        }
        None => {
            if implicit_tx {
                match run_text(c, cx, work, session, "COMMIT")? {
                    Ok(_) => {}
                    Err(e) => {
                        // A COMMIT that returned an error leaves the implicit
                        // transaction's fate uncertain. Roll it back and close
                        // the connection if even that cannot be confirmed.
                        copy_rollback(c, cx, work, session)?;
                        return Ok(Err(e));
                    }
                }
            }
            Ok(Ok(Completed {
                result: empty_result(),
                tag: format!("COPY {rows}"),
                ends_transaction: false,
            }))
        }
    }
}

/// Appends one value to a `COPY TO STDOUT` line, escaping what the text format
/// reserves: the backslash, the tab and the newline.
fn copy_field(out: &mut String, value: &SqlValue) {
    match value {
        SqlValue::Null => out.push_str("\\N"),
        SqlValue::Int(v) => out.push_str(&v.to_string()),
        SqlValue::Bool(v) => out.push(if *v { 't' } else { 'f' }),
        SqlValue::Text(v) => {
            for ch in v.chars() {
                match ch {
                    '\\' => out.push_str("\\\\"),
                    '\t' => out.push_str("\\t"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\u{8}' => out.push_str("\\b"),
                    '\u{c}' => out.push_str("\\f"),
                    '\u{b}' => out.push_str("\\v"),
                    other => out.push(other),
                }
            }
        }
    }
}

fn copy_out(
    c: &mut Stream,
    cx: &Conn,
    tx: &mut TxState,
    session: &mut Session,
    spec: CopySpec,
) -> io::Result<Result<Completed, PgError>> {
    if tx.status == b'E' {
        return Ok(Err(aborted_error()));
    }
    let sql = match (spec.table, spec.query) {
        (Some((table, Some(columns))), _) => {
            format!("SELECT {} FROM {table}", columns.join(", "))
        }
        (Some((table, None)), _) => format!("SELECT * FROM {table}"),
        (None, Some(query)) => query,
        (None, None) => return Ok(Err(PgError::new("42601", "COPY TO requires a table"))),
    };
    let completed = match run_text(c, cx, tx, session, &sql)? {
        Ok(completed) => completed,
        Err(e) => return Ok(Err(e)),
    };
    let result = completed.result;
    let mut body = vec![0u8];
    body.extend_from_slice(&(result.columns.len() as u16).to_be_bytes());
    for _ in &result.columns {
        body.extend_from_slice(&0u16.to_be_bytes());
    }
    packet(c, b'H', &body)?;
    let mut rows = 0u64;
    for row in &result.rows {
        let mut line = String::new();
        for (index, value) in row.iter().enumerate() {
            if index > 0 {
                line.push('\t');
            }
            copy_field(&mut line, value);
        }
        line.push('\n');
        packet(c, b'd', line.as_bytes())?;
        rows += 1;
    }
    packet(c, b'c', &[])?;
    c.flush()?;
    Ok(Ok(Completed {
        result: empty_result(),
        tag: format!("COPY {rows}"),
        ends_transaction: false,
    }))
}

pub fn connection(
    sock: TcpStream,
    st: AppState,
    credentials: Credentials,
    tls: Option<Arc<Tls>>,
) -> io::Result<()> {
    let mut c = Stream::Plain(sock);
    let mut startup = frame(&mut c)?;
    // libpq can probe GSS encryption (80877104) or TLS (80877103) before the
    // startup packet. A GSS probe is always refused; a TLS probe is accepted
    // when the listener has a certificate and refused otherwise, which keeps the
    // cleartext default byte-for-byte the same as before.
    for _ in 0..2 {
        if startup == 80877103u32.to_be_bytes() {
            match &tls {
                Some(tls) => {
                    c.write_all(b"S")?;
                    c.flush()?;
                    c = Stream::upgrade(c, &tls.config)?;
                }
                None => c.write_all(b"N")?,
            }
            startup = frame(&mut c)?;
        } else if startup == 80877104u32.to_be_bytes() {
            c.write_all(b"N")?;
            startup = frame(&mut c)?;
        } else {
            break;
        }
    }
    if startup.len() < 5 || startup[..4] != 196608u32.to_be_bytes() {
        return Err(io::Error::other("unsupported startup protocol"));
    }
    if tls.as_ref().is_some_and(|t| t.required) && matches!(c, Stream::Plain(_)) {
        // The password below would travel in the clear, so a listener that
        // requires TLS refuses instead of degrading.
        error(&mut c, "28000", "TLS is required by this server")?;
        c.flush()?;
        return Ok(());
    }
    let fields: Vec<&str> = std::str::from_utf8(&startup[4..])
        .map_err(io::Error::other)?
        .split('\0')
        .collect();
    let mut params = BTreeMap::new();
    for pair in fields.chunks_exact(2) {
        params.insert(pair[0], pair[1]);
    }
    let user = params.get("user").copied().unwrap_or("");
    let database = params.get("database").copied().unwrap_or("");
    packet(&mut c, b'R', &3u32.to_be_bytes())?; // cleartext password; require trusted network/TLS tunnel
    let mut kind = [0];
    c.read_exact(&mut kind)?;
    if kind[0] != b'p' {
        return Err(io::Error::other("expected password"));
    }
    let pass = frame(&mut c)?;
    let Some(cred) = credentials
        .users
        .get(user)
        .filter(|v| equal(v.secret.as_bytes(), cstr(&pass).unwrap_or("").as_bytes()))
    else {
        error(&mut c, "28P01", "invalid credentials")?;
        return Ok(());
    };
    let Some((project, db)) = database.split_once('/') else {
        error(&mut c, "3D000", "database must be project/database")?;
        return Ok(());
    };
    {
        let cp = match st.control_for_authorization() {
            Ok(cp) => cp,
            Err(crate::ControlAuthError::NotReady) => {
                error(&mut c, "57P03", crate::CONTROL_NOT_READY)?;
                return Ok(());
            }
            Err(crate::ControlAuthError::Stale) => {
                error(&mut c, "57P03", "control plane stale or unavailable")?;
                return Ok(());
            }
        };
        if cp
            .authorize(project, &cred.subject, yadb_control::Role::ReadOnly)
            .is_err()
            || !cp.databases.contains_key(&format!("{project}/{db}"))
        {
            error(&mut c, "42501", "database unavailable or access denied")?;
            return Ok(());
        }
    }
    packet(&mut c, b'R', &0u32.to_be_bytes())?;
    for (k, v) in [
        ("server_version", "14.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        (
            "application_name",
            params.get("application_name").copied().unwrap_or("yadb"),
        ),
    ] {
        packet(&mut c, b'S', format!("{k}\0{v}\0").as_bytes())?;
    }
    let session_id = hex(&random::<16>()?);
    let path = format!("/db/{}/{}/query", url(project), url(db));
    let cx = Conn {
        st: &st,
        project,
        database: db,
        path: &path,
        session: &session_id,
        subject: &cred.subject,
    };
    let mut tx = TxState {
        status: b'I',
        term: 0,
    };
    let mut session = Session::new(&params);
    let mut discard = false;
    ready(&mut c, tx.status)?;
    let result = (|| -> io::Result<()> {
        loop {
            let mut k = [0];
            c.read_exact(&mut k)?;
            let data = frame(&mut c)?;
            // After an extended-protocol error PostgreSQL ignores everything until Sync.
            if discard && !matches!(k[0], b'S' | b'X') {
                continue;
            }
            match k[0] {
                b'X' => return Ok(()),
                b'S' => {
                    discard = false;
                    // Portals do not outlive their transaction; outside a transaction
                    // block Sync ends the implicit transaction that Bind created them in.
                    if tx.status == b'I' {
                        session.portals.clear();
                    }
                    ready(&mut c, tx.status)?;
                }
                b'H' => {}
                b'Q' => {
                    let query = cstr(&data)?;
                    if query.trim().is_empty() {
                        packet(&mut c, b'I', &[])?;
                        ready(&mut c, tx.status)?;
                        continue;
                    }
                    match parse_sql_script(query) {
                        // The whole script is parseable, so it runs exactly as before.
                        Ok(stmts) => {
                            let sources = split(query);
                            if sources.len() != stmts.len() {
                                error(&mut c, "42601", "invalid statement boundaries")?;
                                if tx.status == b'T' {
                                    tx.status = b'E';
                                }
                                ready(&mut c, tx.status)?;
                                continue;
                            }
                            for (stmt, sql) in stmts.iter().zip(sources) {
                                match run_stmt(&mut c, &cx, &mut tx, &mut session, stmt, sql)? {
                                    Ok(completed) => {
                                        rows(&mut c, &completed.result)?;
                                        packet(
                                            &mut c,
                                            b'C',
                                            format!("{}\0", completed.tag).as_bytes(),
                                        )?;
                                        if completed.ends_transaction {
                                            session.portals.clear();
                                        }
                                    }
                                    Err(e) => {
                                        error(&mut c, e.code, &e.message)?;
                                        if tx.status == b'T' {
                                            tx.status = b'E';
                                        }
                                        break;
                                    }
                                }
                            }
                            ready(&mut c, tx.status)?;
                        }
                        // `PREPARE`/`EXECUTE`/`DEALLOCATE` cannot be represented by
                        // the SQL parser (they carry `$n` and a nested statement), so
                        // the script is split and each statement is dispatched on its
                        // own text. The split is planned in full before anything
                        // runs, so a syntax error anywhere still executes nothing -
                        // the same atomicity the whole-script parse gave.
                        Err(parse_error) => {
                            let mut plan: Vec<TextItem> = Vec::new();
                            for sql in split(query) {
                                if blank(sql) {
                                    continue;
                                }
                                match plan_text(sql) {
                                    Ok(item) => plan.push(item),
                                    Err(_) => {
                                        plan.clear();
                                        break;
                                    }
                                }
                            }
                            if plan.is_empty() {
                                error(&mut c, "42601", &parse_error.to_string())?;
                                if tx.status == b'T' {
                                    tx.status = b'E';
                                }
                                ready(&mut c, tx.status)?;
                                continue;
                            }
                            // The client streams COPY data after the response,
                            // so a COPY cannot be planned next to another
                            // statement: its data would arrive while the server
                            // was still running the rest of the script.
                            if plan.len() > 1
                                && plan.iter().any(|item| {
                                    matches!(
                                        item,
                                        TextItem::Session(
                                            SessionCmd::CopyIn(_) | SessionCmd::CopyOut(_)
                                        )
                                    )
                                })
                            {
                                error(
                                    &mut c,
                                    "0A000",
                                    "COPY must be the only statement in a query",
                                )?;
                                if tx.status == b'T' {
                                    tx.status = b'E';
                                }
                                ready(&mut c, tx.status)?;
                                continue;
                            }
                            for item in plan {
                                let outcome = match item {
                                    TextItem::Session(cmd) => {
                                        exec_session_cmd(&mut c, &cx, &mut tx, &mut session, cmd)?
                                    }
                                    TextItem::SessionError(e) => Err(e),
                                    TextItem::Sql(stmt, sql) => {
                                        run_stmt(&mut c, &cx, &mut tx, &mut session, &stmt, sql)?
                                    }
                                };
                                match outcome {
                                    Ok(completed) => {
                                        rows(&mut c, &completed.result)?;
                                        packet(
                                            &mut c,
                                            b'C',
                                            format!("{}\0", completed.tag).as_bytes(),
                                        )?;
                                        if completed.ends_transaction {
                                            session.portals.clear();
                                        }
                                    }
                                    Err(e) => {
                                        error(&mut c, e.code, &e.message)?;
                                        if tx.status == b'T' {
                                            tx.status = b'E';
                                        }
                                        break;
                                    }
                                }
                            }
                            ready(&mut c, tx.status)?;
                        }
                    }
                }
                b'P' | b'B' | b'D' | b'E' | b'C' => {
                    if !extended(&mut c, &cx, &mut tx, &mut session, k[0], &data)? {
                        discard = true;
                        if tx.status == b'T' {
                            tx.status = b'E';
                        }
                    }
                }
                _ => {
                    error(&mut c, "0A000", "unsupported frontend message")?;
                    discard = true;
                }
            }
        }
    })();
    let body = serde_json::to_vec(
        &serde_json::json!({"sql":"ROLLBACK;","session_id":session_id,"force_leader":true}),
    )
    .unwrap();
    let _ = http(
        &st.listen_addr,
        "POST",
        &path,
        &cred.subject,
        st.subject_secret.as_deref(),
        &body,
    );
    result
}

/// True when the text holds only whitespace and comments.
fn blank(sql: &str) -> bool {
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            c if c.is_ascii_whitespace() => i += 1,
            b'-' if b.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut depth = 1u32;
                while depth > 0 {
                    if i >= b.len() {
                        return false;
                    }
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            _ => return false,
        }
    }
    true
}

/// Splits a query string at top-level semicolons, ignoring semicolons inside
/// single-quoted strings and line comments. Whitespace-only and comment-only
/// segments are dropped, so the result lines up one-to-one with the statements
/// the SQL parser produces for the same text.
fn split(sql: &str) -> Vec<&str> {
    let b = sql.as_bytes();
    let mut quoted = false;
    let mut ident = false;
    let mut comment = false;
    let mut block = 0u32;
    let mut start = 0;
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if comment {
            if b[i] == b'\n' {
                comment = false;
            }
        } else if block > 0 {
            if b[i..].starts_with(b"/*") {
                block += 1;
                i += 1;
            } else if b[i..].starts_with(b"*/") {
                block -= 1;
                i += 1;
            }
        } else if ident {
            if b[i] == b'"' {
                if b.get(i + 1) == Some(&b'"') {
                    i += 1;
                } else {
                    ident = false;
                }
            }
        } else if !quoted && b[i..].starts_with(b"--") {
            comment = true;
            i += 1;
        } else if !quoted && b[i..].starts_with(b"/*") {
            block = 1;
            i += 1;
        } else if !quoted && b[i] == b'"' {
            ident = true;
        } else if b[i] == b'\'' {
            if quoted && b.get(i + 1) == Some(&b'\'') {
                i += 1;
            } else {
                quoted = !quoted;
            }
        } else if b[i] == b';' && !quoted {
            let segment = sql[start..i].trim();
            if !blank(segment) {
                out.push(segment);
            }
            start = i + 1;
        }
        i += 1;
    }
    let segment = sql[start..].trim();
    if !blank(segment) {
        out.push(segment);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_not_ready_answer_is_retryable_and_an_unknown_outcome_is_not() {
        // The gateway answers both of these with 503, so the status cannot be
        // what distinguishes them. `x-yadb-retry: do-not-retry` is the marker
        // `forward_error_response` adds for a write whose commit outcome the
        // node could not determine.
        let plain = "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\n";
        assert_eq!(
            pg_sqlstate_for(503, plain),
            "57P03",
            "a plain 503 is \"not ready\", so a client may retry"
        );

        let unknown = "HTTP/1.1 503 Service Unavailable\r\n\
                       x-yadb-write-outcome: unknown\r\n\
                       x-yadb-retry: do-not-retry\r\n";
        assert_eq!(
            pg_sqlstate_for(503, unknown),
            "XX000",
            "an unknown write outcome must not be reported as retryable"
        );

        // Header names arrive lowercased from axum, but the check must not
        // depend on that: HTTP header names are case-insensitive.
        let mixed = "HTTP/1.1 503 Service Unavailable\r\nX-Yadb-Retry: Do-Not-Retry\r\n";
        assert_eq!(pg_sqlstate_for(503, mixed), "XX000");

        assert_eq!(pg_sqlstate_for(403, plain), "42501");
        assert_eq!(pg_sqlstate_for(400, plain), "XX000");
    }

    fn values<'a>(items: &[Option<&'a str>]) -> Vec<Option<&'a str>> {
        items.to_vec()
    }

    #[test]
    fn splitting() {
        assert_eq!(
            split("BEGIN; INSERT INTO t VALUES ('a;''b'); -- x;\n COMMIT;").len(),
            3
        );
    }

    #[test]
    fn quoted_text_and_comments_are_not_placeholders() {
        let sql = "SELECT '$1', \"$2\", $1 -- $2\n, /* $3 /* $4 */ $5 */ $10";
        let all = values(&[
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]);
        assert_eq!(
            substitute(sql, &all, &[]).unwrap(),
            "SELECT '$1', \"$2\", 'a' -- $2\n, /* $3 /* $4 */ $5 */ 'j'"
        );
    }

    #[test]
    fn placeholder_numbers_do_not_confuse_prefixes() {
        let all = values(&[
            Some("one"),
            Some("two"),
            Some("three"),
            Some("four"),
            Some("five"),
            Some("six"),
            Some("seven"),
            Some("eight"),
            Some("nine"),
            Some("ten"),
        ]);
        assert_eq!(
            substitute("SELECT $1, $10", &all, &[]).unwrap(),
            "SELECT 'one', 'ten'"
        );
    }

    #[test]
    fn injection_value_stays_a_quoted_string() {
        let injected = values(&[Some("'; DROP TABLE t; --")]);
        assert_eq!(
            substitute("INSERT INTO t VALUES ($1)", &injected, &[25]).unwrap(),
            "INSERT INTO t VALUES ('''; DROP TABLE t; --')"
        );
        let quoted = values(&[Some("it's fine")]);
        assert_eq!(
            substitute("SELECT $1", &quoted, &[25]).unwrap(),
            "SELECT 'it''s fine'"
        );
    }

    #[test]
    fn null_and_typed_literals() {
        assert_eq!(
            substitute("SELECT $1", &values(&[None]), &[23]).unwrap(),
            "SELECT NULL"
        );
        assert_eq!(
            substitute("SELECT $1", &values(&[Some("42")]), &[23]).unwrap(),
            "SELECT 42"
        );
        assert_eq!(
            substitute("SELECT $1", &values(&[Some("t")]), &[16]).unwrap(),
            "SELECT true"
        );
        assert_eq!(
            substitute("SELECT $1", &values(&[Some("1.5")]), &[701]).unwrap(),
            "SELECT 1.5"
        );
    }

    #[test]
    fn invalid_numeric_and_boolean_values_are_rejected() {
        assert_eq!(
            substitute("SELECT $1", &values(&[Some("12x")]), &[23])
                .unwrap_err()
                .code,
            "22P02"
        );
        assert_eq!(
            substitute("SELECT $1", &values(&[Some("yes")]), &[16])
                .unwrap_err()
                .code,
            "22P02"
        );
        assert_eq!(
            substitute("SELECT $1", &values(&[Some("inf")]), &[701])
                .unwrap_err()
                .code,
            "22P02"
        );
    }

    #[test]
    fn unspecified_oid_infers_integer_float_boolean_text() {
        let cases = [
            ("42", "42"),
            ("-7", "-7"),
            ("1.5", "1.5"),
            ("1e3", "1e3"),
            ("true", "true"),
            ("F", "false"),
            ("hello", "'hello'"),
            ("", "''"),
        ];
        for (value, expected) in cases {
            assert_eq!(
                substitute("SELECT $1", &values(&[Some(value)]), &[0]).unwrap(),
                format!("SELECT {expected}"),
                "{value}"
            );
        }
    }

    #[test]
    fn missing_and_extra_parameters_are_rejected() {
        let missing =
            substitute("SELECT $1, $3", &values(&[Some("a"), Some("b")]), &[]).unwrap_err();
        assert_eq!(missing.code, "42P02");
        assert!(missing.message.contains("$3"), "{}", missing.message);
        let extra = substitute("SELECT $1", &values(&[Some("a"), Some("b")]), &[]).unwrap_err();
        assert_eq!(extra.code, "08P01");
        assert_eq!(
            substitute("SELECT $0", &values(&[]), &[]).unwrap_err().code,
            "42P02"
        );
    }

    #[test]
    fn statement_boundaries_ignore_quotes_and_comments() {
        assert_eq!(scan("SELECT 1; SELECT 2;").unwrap().statements, 2);
        assert_eq!(scan("SELECT ';'; -- ;\n").unwrap().statements, 1);
        assert_eq!(scan("/* ; */ SELECT 1").unwrap().statements, 1);
        assert_eq!(scan("  ;; ").unwrap().statements, 0);
        assert_eq!(scan("").unwrap().statements, 0);
    }

    #[test]
    fn split_keeps_session_commands_and_drops_blank_segments() {
        let script = "PREPARE p (int) AS SELECT v FROM t WHERE id = $1; EXECUTE p(1); -- tail;\n";
        assert_eq!(
            split(script),
            vec![
                "PREPARE p (int) AS SELECT v FROM t WHERE id = $1",
                "EXECUTE p(1)"
            ]
        );
        assert_eq!(split("SELECT 1;;  "), vec!["SELECT 1"]);
        assert_eq!(
            split("SELECT 1 /* ; not a split */; SELECT 2"),
            vec!["SELECT 1 /* ; not a split */", "SELECT 2"]
        );
        assert_eq!(
            split("/* nested /* ; */ ; */ SELECT 3"),
            vec!["/* nested /* ; */ ; */ SELECT 3"]
        );
        // A semicolon inside a quoted identifier is data, not a boundary.
        assert_eq!(
            split("SELECT \"a;b\" FROM t; SELECT 2"),
            vec!["SELECT \"a;b\" FROM t", "SELECT 2"]
        );
        assert_eq!(
            split("SELECT 'it''s; fine' FROM t"),
            vec!["SELECT 'it''s; fine' FROM t"]
        );
        // A double quote inside a string literal is data: it must not open a
        // quoted identifier that swallows the rest of the script.
        assert_eq!(
            split("SELECT 'a\"b;c' FROM t; SELECT 2"),
            vec!["SELECT 'a\"b;c' FROM t", "SELECT 2"]
        );
        assert!(split("  -- only a comment\n").iter().all(|s| blank(s)));
        assert!(blank("/* nested /* comment */ */"));
        assert!(!blank("SELECT 1"));
    }

    #[test]
    fn prepare_execute_and_deallocate_parse() {
        assert_eq!(
            session_cmd("PREPARE p (int, text) AS SELECT v FROM t WHERE id = $1 AND n = $2")
                .unwrap()
                .map(|cmd| match cmd {
                    SessionCmd::Prepare { name, oids, sql } => (name, oids, sql),
                    other => panic!("{other:?}"),
                }),
            Some((
                "p".to_string(),
                vec![23, 25],
                "SELECT v FROM t WHERE id = $1 AND n = $2".to_string()
            ))
        );
        assert_eq!(
            session_cmd("prepare p as select 1")
                .unwrap()
                .map(|cmd| match cmd {
                    SessionCmd::Prepare { name, oids, sql } => (name, oids, sql),
                    other => panic!("{other:?}"),
                }),
            Some(("p".to_string(), vec![], "select 1".to_string()))
        );
        assert_eq!(
            session_cmd("EXECUTE p(1, 'it''s')")
                .unwrap()
                .map(|cmd| match cmd {
                    SessionCmd::Execute { name, args } => (name, args),
                    other => panic!("{other:?}"),
                }),
            Some((
                "p".to_string(),
                vec![SqlValue::Int(1), SqlValue::Text("it's".into())]
            ))
        );
        assert_eq!(
            session_cmd("EXECUTE p()").unwrap().map(|cmd| match cmd {
                SessionCmd::Execute { name, args } => (name, args),
                other => panic!("{other:?}"),
            }),
            Some(("p".to_string(), vec![]))
        );
        assert_eq!(
            session_cmd("DEALLOCATE p").unwrap().map(|cmd| match cmd {
                SessionCmd::Deallocate { all, name } => (all, name),
                other => panic!("{other:?}"),
            }),
            Some((false, "p".to_string()))
        );
        assert_eq!(
            session_cmd("DEALLOCATE PREPARE ALL")
                .unwrap()
                .map(|cmd| match cmd {
                    SessionCmd::Deallocate { all, .. } => all,
                    other => panic!("{other:?}"),
                }),
            Some(true)
        );
        // Anything else is left to the SQL parser.
        for sql in ["SELECT 1", "SET x = 1", "PREPARE", "EXECUTE"] {
            assert!(
                session_cmd(sql).is_err() || session_cmd(sql).unwrap().is_none(),
                "{sql}"
            );
        }
        assert!(session_cmd("SELECT 1").unwrap().is_none());
        assert!(session_cmd("SET client_encoding = 'UTF8'")
            .unwrap()
            .is_none());
    }

    #[test]
    fn session_commands_refuse_what_they_cannot_do() {
        for (sql, code) in [
            ("PREPARE TRANSACTION 'gid'", "0A000"),
            ("PREPARE p (int) AS SELECT 1", "42P02"),
            ("PREPARE p (no_such_type) AS SELECT 1", "42704"),
            ("EXECUTE p(1 + 1)", "0A000"),
            ("EXECUTE p(1) trailing", "42601"),
            ("DEALLOCATE", "42601"),
        ] {
            let err = match session_cmd(sql) {
                Ok(cmd) => match cmd {
                    // Arity is checked when the statement is executed, not parsed.
                    Some(SessionCmd::Prepare { name, oids, sql }) => prepare_sql(
                        &mut Session::new(&BTreeMap::<&str, &str>::new()),
                        name,
                        oids,
                        sql,
                    )
                    .unwrap_err(),
                    other => panic!("{sql} parsed as {other:?}"),
                },
                Err(e) => e,
            };
            assert_eq!(err.code, code, "{sql}: {}", err.message);
        }
    }

    #[test]
    fn execute_values_are_substituted_as_literals() {
        assert_eq!(
            substitute_values(
                "SELECT v FROM t WHERE id = $1 AND n = $2",
                &[SqlValue::Int(1), SqlValue::Text("it's".into())]
            )
            .unwrap(),
            "SELECT v FROM t WHERE id = 1 AND n = 'it''s'"
        );
        assert_eq!(
            substitute_values("SELECT $1", &[SqlValue::Null]).unwrap(),
            "SELECT NULL"
        );
        assert_eq!(
            substitute_values("SELECT $1", &[SqlValue::Bool(false)]).unwrap(),
            "SELECT false"
        );
        assert_eq!(
            substitute_values("SELECT $1", &[]).unwrap_err().code,
            "08P01"
        );
    }

    #[test]
    fn declared_parameter_types_are_checked() {
        let ok = [SqlValue::Int(1), SqlValue::Text("x".into())];
        assert!(check_arg_types("p", &ok, &[23, 25]).is_ok());
        assert!(check_arg_types("p", &ok, &[]).is_ok());
        assert_eq!(
            check_arg_types("p", &[SqlValue::Text("1".into())], &[23])
                .unwrap_err()
                .code,
            "22P02"
        );
        assert_eq!(
            check_arg_types("p", &[SqlValue::Int(1)], &[16])
                .unwrap_err()
                .code,
            "22P02"
        );
    }

    #[test]
    fn a_named_statement_cannot_be_parsed_twice_but_the_unnamed_one_can() {
        fn parse(name: &str, sql: &str) -> Vec<u8> {
            let mut data = Vec::new();
            data.extend_from_slice(name.as_bytes());
            data.push(0);
            data.extend_from_slice(sql.as_bytes());
            data.push(0);
            data.extend_from_slice(&0u16.to_be_bytes());
            data
        }
        let mut statements = BTreeMap::new();
        parse_ext(&parse("p", "SELECT $1"), &mut statements).unwrap();
        // A second Parse under a taken name is refused, and the statement that
        // already exists is left exactly as it was.
        let error = parse_ext(&parse("p", "SELECT $2"), &mut statements).unwrap_err();
        assert_eq!(error.code, "42P05", "{}", error.message);
        assert_eq!(
            statements.get("p").map(|p| p.sql.as_str()),
            Some("SELECT $1")
        );
        // The unnamed statement is replaced freely, as in PostgreSQL.
        parse_ext(&parse("", "SELECT 1"), &mut statements).unwrap();
        parse_ext(&parse("", "SELECT 2"), &mut statements).unwrap();
        assert_eq!(statements.get("").map(|p| p.sql.as_str()), Some("SELECT 2"));
    }

    #[test]
    fn split_script_is_planned_before_anything_runs() {
        // A session command and a plain statement are both accepted...
        assert!(matches!(
            plan_text("PREPARE p AS SELECT 1"),
            Ok(TextItem::Session(SessionCmd::Prepare { .. }))
        ));
        assert!(matches!(
            plan_text("SELECT 1"),
            Ok(TextItem::Sql(SqlStmt::SelectNoFrom { .. }, "SELECT 1"))
        ));
        // ...while an unparseable segment rejects the whole script, so no
        // earlier statement can have run.
        assert!(plan_text("SELECT FROM WHERE").is_err());
        assert!(plan_text("").is_err());
        // A session command that is itself invalid keeps its own error.
        assert!(matches!(
            plan_text("PREPARE TRANSACTION 'gid'"),
            Ok(TextItem::SessionError(e)) if e.code == "0A000"
        ));
    }

    #[test]
    fn session_commands_skip_leading_comments() {
        assert_eq!(
            session_cmd("-- a note\nPREPARE p AS SELECT 1")
                .unwrap()
                .map(|cmd| match cmd {
                    SessionCmd::Prepare { name, .. } => name,
                    other => panic!("{other:?}"),
                }),
            Some("p".to_string())
        );
        assert_eq!(
            session_cmd("/* nested /* x */ y */ EXECUTE p(1)")
                .unwrap()
                .map(|cmd| match cmd {
                    SessionCmd::Execute { name, .. } => name,
                    other => panic!("{other:?}"),
                }),
            Some("p".to_string())
        );
    }

    #[test]
    fn session_statement_tags_match_postgres() {
        let empty = empty_result();
        let one_row = QueryResult {
            columns: vec!["?column?".into()],
            rows: vec![vec![SqlValue::Int(1)]],
            rows_affected: 0,
        };
        for (sql, expected) in [
            ("SET x = 1", "SET"),
            ("SET x = DEFAULT", "SET"),
            ("RESET x", "RESET"),
            ("SHOW x", "SHOW"),
            ("SELECT 1", "SELECT 1"),
        ] {
            let stmt = yadb_db::parse_sql(sql).unwrap();
            let res = if matches!(stmt, SqlStmt::SelectNoFrom { .. }) {
                &one_row
            } else {
                &empty
            };
            assert_eq!(tag(&stmt, res, false), expected, "{sql}");
        }
    }

    #[test]
    fn copy_parses_table_and_query_forms() {
        match session_cmd("COPY t FROM STDIN").unwrap() {
            Some(SessionCmd::CopyIn(spec)) => {
                assert_eq!(spec.table, Some(("t".to_string(), None)));
                assert!(spec.query.is_none());
            }
            other => panic!("{other:?}"),
        }
        match session_cmd("COPY t (a, b) FROM STDIN WITH (FORMAT text)").unwrap() {
            Some(SessionCmd::CopyIn(spec)) => assert_eq!(
                spec.table,
                Some(("t".to_string(), Some(vec!["a".into(), "b".into()])))
            ),
            other => panic!("{other:?}"),
        }
        match session_cmd("COPY (SELECT a, 'b)' FROM t WHERE a > 1) TO STDOUT").unwrap() {
            Some(SessionCmd::CopyOut(spec)) => {
                assert_eq!(
                    spec.query.as_deref(),
                    Some("SELECT a, 'b)' FROM t WHERE a > 1")
                );
                assert!(spec.table.is_none());
            }
            other => panic!("{other:?}"),
        }
        match session_cmd("COPY t TO STDOUT").unwrap() {
            Some(SessionCmd::CopyOut(spec)) => {
                assert_eq!(spec.table, Some(("t".to_string(), None)))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn copy_refuses_what_it_cannot_do() {
        for (sql, code) in [
            ("COPY t FROM '/etc/passwd'", "0A000"),
            ("COPY t TO '/tmp/out'", "0A000"),
            ("COPY t FROM STDIN WITH (FORMAT csv)", "0A000"),
            ("COPY t FROM STDIN WITH (DELIMITER ',')", "0A000"),
            ("COPY (SELECT 1) FROM STDIN", "42601"),
            ("COPY t FROM", "42601"),
            ("COPY t (a,) FROM STDIN", "42601"),
        ] {
            let error = match session_cmd(sql) {
                Ok(other) => panic!("{sql} parsed as {other:?}"),
                Err(e) => e,
            };
            assert_eq!(error.code, code, "{sql}");
        }
    }

    #[test]
    fn copy_text_fields_round_trip() {
        // Escapes the text format defines, including an octal and a hex byte.
        assert_eq!(
            copy_unescape(b"a\\b\\tc\\nd\\\\e\\101\\x42").unwrap(),
            b"a\x08\tc\nd\\eAB"
        );
        let mut line = String::new();
        copy_field(&mut line, &SqlValue::Text("a\tb\nc\\d".into()));
        line.push('\t');
        copy_field(&mut line, &SqlValue::Null);
        line.push('\t');
        copy_field(&mut line, &SqlValue::Int(-3));
        line.push('\t');
        copy_field(&mut line, &SqlValue::Bool(true));
        assert_eq!(line, "a\\tb\\nc\\\\d\t\\N\t-3\tt");
    }

    #[test]
    fn copy_rows_are_typed_and_bounded() {
        let columns = vec![
            CopyColumn {
                name: "id".into(),
                kind: CopyType::Int,
            },
            CopyColumn {
                name: "name".into(),
                kind: CopyType::Text,
            },
            CopyColumn {
                name: "ok".into(),
                kind: CopyType::Bool,
            },
        ];
        assert_eq!(
            copy_row(b"7\tit's\tt", &columns).unwrap(),
            vec!["7".to_string(), "'it''s'".to_string(), "true".to_string()]
        );
        assert_eq!(
            copy_row(b"\\N\t\\N\t\\N", &columns).unwrap(),
            vec!["NULL".to_string(), "NULL".to_string(), "NULL".to_string()]
        );
        // A text column that looks numeric must stay text: typing is by the
        // declared column type, never by the value.
        assert_eq!(copy_row(b"1\t007\tf", &columns).unwrap()[1], "'007'");
        assert_eq!(copy_row(b"x\ta\tt", &columns).unwrap_err().code, "22P02");
        assert_eq!(copy_row(b"1\ta", &columns).unwrap_err().code, "22P04");
        assert_eq!(
            copy_row(b"1\ta\tt\textra", &columns).unwrap_err().code,
            "22P04"
        );
        assert_eq!(copy_unescape(b"bad\\x").unwrap_err().code, "22P04");
        // An octal escape above one byte is refused rather than truncated.
        assert_eq!(copy_unescape(b"\\777").unwrap_err().code, "22P04");
    }

    #[test]
    fn copy_line_terminators_are_stripped_without_eating_data() {
        let mut lf = b"1\talpha\n".to_vec();
        copy_strip_eol(&mut lf);
        assert_eq!(lf, b"1\talpha");
        let mut crlf = b"1\talpha\r\n".to_vec();
        copy_strip_eol(&mut crlf);
        assert_eq!(crlf, b"1\talpha");
        // A lone CR before the LF is the terminator, not a second data byte.
        let mut empty = b"\r\n".to_vec();
        copy_strip_eol(&mut empty);
        assert!(empty.is_empty());
    }

    #[test]
    fn copy_batches_build_one_insert() {
        let columns = vec![
            CopyColumn {
                name: "id".into(),
                kind: CopyType::Int,
            },
            CopyColumn {
                name: "name".into(),
                kind: CopyType::Text,
            },
        ];
        let batch = vec![
            vec!["1".to_string(), "'a'".to_string()],
            vec!["2".to_string(), "NULL".to_string()],
        ];
        assert_eq!(
            copy_insert("t", &columns, &batch),
            "INSERT INTO t (id,name) VALUES (1,'a'),(2,NULL)"
        );
    }
}
