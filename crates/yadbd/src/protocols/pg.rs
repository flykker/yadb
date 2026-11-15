//! PostgreSQL v3 wire frontend: Simple Query and the extended query protocol
//! (Parse/Bind/Describe/Execute/Close/Flush/Sync), text format only.
//!
//! The engine does not type parameters. `Bind` decodes text values using the OIDs
//! supplied by `Parse` and substitutes them into the SQL text as literals. OID 0 is
//! inferred as integer, then float, then boolean, then text. The substitution is
//! quote/comment aware so a parameter value can never become SQL syntax.
use super::*;
use crate::AppState;
use yadb_db::{parse_sql_script, QueryResult, SqlStmt, SqlValue};

/// Bounds on session state; exceeding either answers a clean error instead of growing.
const MAX_PREPARED: usize = 64;
const MAX_PORTALS: usize = 64;
const MAX_NAME: usize = 256;
const MAX_SQL: usize = 64 * 1024;
const MAX_PARAMS: u32 = 32_767;

fn packet(c: &mut TcpStream, kind: u8, data: &[u8]) -> io::Result<()> {
    c.write_all(&[kind])?;
    c.write_all(&((data.len() + 4) as u32).to_be_bytes())?;
    c.write_all(data)
}
fn error(c: &mut TcpStream, code: &str, msg: &str) -> io::Result<()> {
    packet(
        c,
        b'E',
        format!("SERROR\0VERROR\0C{code}\0M{}\0\0", msg.replace('\0', "")).as_bytes(),
    )
}
fn ready(c: &mut TcpStream, status: u8) -> io::Result<()> {
    packet(c, b'Z', &[status])
}
fn frame(c: &mut TcpStream) -> io::Result<Vec<u8>> {
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
fn data_rows(c: &mut TcpStream, res: &QueryResult) -> io::Result<()> {
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
fn rows(c: &mut TcpStream, res: &QueryResult) -> io::Result<()> {
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
    path: &'a str,
    session: &'a str,
    subject: &'a str,
}
#[derive(Clone, Copy)]
struct TxState {
    status: u8,
    term: u64,
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
    // PostgreSQL replaces a statement when the same name is parsed again.
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
fn describe_sql(c: &mut TcpStream, cx: &Conn, tx: &TxState, sql: &str) -> io::Result<()> {
    if tx.status == b'E' {
        return packet(c, b'n', &[]);
    }
    let Ok(statements) = parse_sql_script(sql) else {
        return packet(c, b'n', &[]);
    };
    if statements.len() != 1
        || !matches!(
            statements[0],
            SqlStmt::Select { .. } | SqlStmt::ShowTables | SqlStmt::ShowIndexes { .. }
        )
    {
        return packet(c, b'n', &[]);
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
    let (code, body) = http(&cx.st.listen_addr, "POST", cx.path, cx.subject, &body)?;
    let Ok(result) = serde_json::from_slice::<QueryResult>(&body) else {
        return packet(c, b'n', &[]);
    };
    if code != 200 || result.columns.is_empty() {
        return packet(c, b'n', &[]);
    }
    packet(c, b'T', &row_description(&result.columns))
}

fn describe_ext(
    c: &mut TcpStream,
    cx: &Conn,
    tx: &TxState,
    statements: &BTreeMap<String, Prepared>,
    portals: &BTreeMap<String, Portal>,
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
            let Some(prepared) = statements.get(name) else {
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
            describe_sql(c, cx, tx, &sql)?;
        }
        b'P' => {
            let Some(portal) = portals.get(name) else {
                return Ok(Err(PgError::new(
                    "34000",
                    format!("portal \"{name}\" does not exist"),
                )));
            };
            describe_sql(c, cx, tx, &portal.sql)?;
        }
        _ => return Ok(Err(PgError::new("08P01", "invalid Describe target"))),
    }
    Ok(Ok(()))
}

fn execute_ext(
    c: &mut TcpStream,
    cx: &Conn,
    tx: &mut TxState,
    portals: &mut BTreeMap<String, Portal>,
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
    let Some(portal) = portals.get(name) else {
        return Ok(Err(PgError::new(
            "34000",
            format!("portal \"{name}\" does not exist"),
        )));
    };
    let statements = match parse_sql_script(&portal.sql) {
        Ok(statements) => statements,
        Err(e) => return Ok(Err(PgError::new("42601", e.to_string()))),
    };
    if statements.len() != 1 {
        return Ok(Err(PgError::new(
            "42601",
            "cannot execute multiple commands in an extended query",
        )));
    }
    let statement = &statements[0];
    let aborted_commit = tx.status == b'E' && matches!(statement, SqlStmt::Commit);
    let result = match exec_one(cx, tx, statement, &portal.sql)? {
        Ok(result) => result,
        Err(e) => return Ok(Err(e)),
    };
    data_rows(c, &result)?;
    packet(
        c,
        b'C',
        format!("{}\0", tag(statement, &result, aborted_commit)).as_bytes(),
    )?;
    // Portals do not outlive the transaction that created them.
    if matches!(statement, SqlStmt::Commit | SqlStmt::Rollback) {
        portals.clear();
    }
    Ok(Ok(()))
}

/// Dispatch one extended-protocol message. Returns false when an ErrorResponse was
/// sent and the caller must ignore messages until Sync.
fn extended(
    c: &mut TcpStream,
    cx: &Conn,
    tx: &mut TxState,
    statements: &mut BTreeMap<String, Prepared>,
    portals: &mut BTreeMap<String, Portal>,
    kind: u8,
    data: &[u8],
) -> io::Result<bool> {
    let result = match kind {
        b'P' => parse_ext(data, statements),
        b'B' => bind_ext(data, statements, portals),
        b'C' => close_ext(data, statements, portals),
        b'D' => describe_ext(c, cx, tx, statements, portals, data)?,
        b'E' => execute_ext(c, cx, tx, portals, data)?,
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
    let (code, body) = http(&cx.st.listen_addr, "POST", cx.path, cx.subject, &body)?;
    if code != 200 {
        if tx.status == b'T' {
            tx.status = b'E';
        }
        return Ok(Err(PgError::new(
            if code == 403 { "42501" } else { "XX000" },
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
        _ => "SHOW".into(),
    }
}

pub fn connection(mut c: TcpStream, st: AppState, credentials: Credentials) -> io::Result<()> {
    let mut startup = frame(&mut c)?;
    // libpq can probe either GSS encryption or TLS. TLS termination is external in this version.
    for _ in 0..2 {
        if startup == 80877103u32.to_be_bytes() || startup == 80877104u32.to_be_bytes() {
            c.write_all(b"N")?;
            startup = frame(&mut c)?;
        }
    }
    if startup.len() < 5 || startup[..4] != 196608u32.to_be_bytes() {
        return Err(io::Error::other("unsupported startup protocol"));
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
            Err(_) => {
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
        ("application_name", "yadb"),
    ] {
        packet(&mut c, b'S', format!("{k}\0{v}\0").as_bytes())?;
    }
    let session = hex(&random::<16>()?);
    let path = format!("/db/{}/{}/query", url(project), url(db));
    let cx = Conn {
        st: &st,
        project,
        path: &path,
        session: &session,
        subject: &cred.subject,
    };
    let mut tx = TxState {
        status: b'I',
        term: 0,
    };
    let mut statements = BTreeMap::new();
    let mut portals = BTreeMap::new();
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
                        portals.clear();
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
                    let stmts = match parse_sql_script(query) {
                        Ok(v) => v,
                        Err(e) => {
                            error(&mut c, "42601", &e.to_string())?;
                            if tx.status == b'T' {
                                tx.status = b'E';
                            }
                            ready(&mut c, tx.status)?;
                            continue;
                        }
                    };
                    // Split on the parser's own tokens is unnecessary: execute each parsed statement by
                    // serializing its source slices using quote-aware statement boundaries below.
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
                        let aborted_commit = tx.status == b'E' && matches!(stmt, SqlStmt::Commit);
                        let res = match exec_one(&cx, &mut tx, stmt, sql)? {
                            Ok(res) => res,
                            Err(e) => {
                                error(&mut c, e.code, &e.message)?;
                                break;
                            }
                        };
                        rows(&mut c, &res)?;
                        packet(
                            &mut c,
                            b'C',
                            format!("{}\0", tag(stmt, &res, aborted_commit)).as_bytes(),
                        )?;
                        if matches!(stmt, SqlStmt::Commit | SqlStmt::Rollback) {
                            portals.clear();
                        }
                    }
                    ready(&mut c, tx.status)?;
                }
                b'P' | b'B' | b'D' | b'E' | b'C' => {
                    if !extended(
                        &mut c,
                        &cx,
                        &mut tx,
                        &mut statements,
                        &mut portals,
                        k[0],
                        &data,
                    )? {
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
        &serde_json::json!({"sql":"ROLLBACK;","session_id":session,"force_leader":true}),
    )
    .unwrap();
    let _ = http(&st.listen_addr, "POST", &path, &cred.subject, &body);
    result
}

fn split(sql: &str) -> Vec<&str> {
    let b = sql.as_bytes();
    let mut quoted = false;
    let mut comment = false;
    let mut start = 0;
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if comment {
            if b[i] == b'\n' {
                comment = false;
            }
        } else if !quoted && b[i..].starts_with(b"--") {
            comment = true;
            i += 1;
        } else if b[i] == b'\'' {
            if quoted && b.get(i + 1) == Some(&b'\'') {
                i += 1;
            } else {
                quoted = !quoted;
            }
        } else if b[i] == b';' && !quoted {
            let s = &sql[start..i];
            if parse_sql_script(s).is_ok() {
                out.push(s);
            }
            start = i + 1;
        }
        i += 1;
    }
    let s = &sql[start..];
    if parse_sql_script(s).is_ok() {
        out.push(s);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
