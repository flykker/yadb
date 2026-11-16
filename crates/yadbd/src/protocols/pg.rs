//! PostgreSQL v3 wire frontend: Simple Query and the extended query protocol
//! (Parse/Bind/Describe/Execute/Close/Flush/Sync), text format only.
//!
//! The engine does not type parameters. `Bind` decodes text values using the OIDs
//! supplied by `Parse` and substitutes them into the SQL text as literals. OID 0 is
//! inferred as integer, then float, then boolean, then text. The substitution is
//! quote/comment aware so a parameter value can never become SQL syntax.
use super::*;
use crate::AppState;
use yadb_db::{
    eval_scalar_select, parse_sql_script, parse_sql_value, setting_default, QueryResult,
    ScalarContext, SqlStmt, SqlValue,
};

/// Bounds on session state; exceeding either answers a clean error instead of growing.
const MAX_PREPARED: usize = 64;
const MAX_PORTALS: usize = 64;
const MAX_SETTINGS: usize = 256;
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
    Ok(None)
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

fn report_name(name: &str) -> &str {
    match name {
        "datestyle" => "DateStyle",
        "timezone" => "TimeZone",
        _ => name,
    }
}

/// Echoes a `GUC_REPORT` parameter change as a ParameterStatus message, which is
/// how drivers observe `SET client_encoding` and friends.
fn report_setting(c: &mut TcpStream, name: &str, value: &str) -> io::Result<()> {
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
    c: &mut TcpStream,
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
        SqlStmt::SelectNoFrom { .. } => format!("SELECT {}", res.rows.len()),
        SqlStmt::Set { .. } => "SET".into(),
        SqlStmt::SetDefault { .. } => "SET".into(),
        SqlStmt::Reset { .. } => "RESET".into(),
        SqlStmt::ShowSetting { .. } | SqlStmt::ShowAll => "SHOW".into(),
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
    let _ = http(&st.listen_addr, "POST", &path, &cred.subject, &body);
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
}
