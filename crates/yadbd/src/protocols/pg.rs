//! PostgreSQL v3 Simple Query frontend (text results, no extended-query/prepared protocol).
use super::*;
use crate::AppState;
use yadb_db::{parse_sql_script, QueryResult, SqlStmt, SqlValue};

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
fn rows(c: &mut TcpStream, res: &QueryResult) -> io::Result<()> {
    if res.columns.is_empty() {
        return Ok(());
    }
    let mut desc = Vec::new();
    desc.extend_from_slice(&(res.columns.len() as u16).to_be_bytes());
    for name in &res.columns {
        desc.extend_from_slice(name.as_bytes());
        desc.push(0);
        desc.extend_from_slice(&0u32.to_be_bytes());
        desc.extend_from_slice(&0u16.to_be_bytes());
        desc.extend_from_slice(&25u32.to_be_bytes()); // text; avoids type inference from an empty result
        desc.extend_from_slice(&(-1i16).to_be_bytes());
        desc.extend_from_slice(&(-1i32).to_be_bytes());
        desc.extend_from_slice(&0u16.to_be_bytes());
    }
    packet(c, b'T', &desc)?;
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
        let cp = st.control.read().unwrap();
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
    let mut status = b'I';
    let mut tx_term = 0;
    let mut discard = false;
    ready(&mut c, status)?;
    let result = (|| -> io::Result<()> {
        loop {
            let mut k = [0];
            c.read_exact(&mut k)?;
            let data = frame(&mut c)?;
            match k[0] {
                b'X' => return Ok(()),
                b'S' => {
                    discard = false;
                    ready(&mut c, status)?;
                }
                b'H' => {}
                b'Q' => {
                    let query = cstr(&data)?;
                    if query.trim().is_empty() {
                        packet(&mut c, b'I', &[])?;
                        ready(&mut c, status)?;
                        continue;
                    }
                    let stmts = match parse_sql_script(query) {
                        Ok(v) => v,
                        Err(e) => {
                            error(&mut c, "42601", &e.to_string())?;
                            if status == b'T' {
                                status = b'E';
                            }
                            ready(&mut c, status)?;
                            continue;
                        }
                    };
                    // Split on the parser's own tokens is unnecessary: execute each parsed statement by
                    // serializing its source slices using quote-aware statement boundaries below.
                    let sources = split(query);
                    if sources.len() != stmts.len() {
                        error(&mut c, "42601", "invalid statement boundaries")?;
                        ready(&mut c, status)?;
                        continue;
                    }
                    for (stmt, sql) in stmts.iter().zip(sources) {
                        let rollback = matches!(stmt, SqlStmt::Rollback);
                        if status == b'E' && !rollback {
                            error(&mut c, "25P02", "transaction aborted; ROLLBACK required")?;
                            break;
                        }
                        let term = st.meta.role_info().2;
                        if status == b'T' && tx_term != term && !rollback {
                            status = b'E';
                            error(
                                &mut c,
                                "40001",
                                "leader term changed; transaction must be restarted",
                            )?;
                            break;
                        }
                        let body=serde_json::to_vec(&serde_json::json!({"sql":sql,"session_id":session,"force_leader":true,"require_transaction":status==b'T' && !rollback})).map_err(io::Error::other)?;
                        let (code, body) =
                            http(&st.listen_addr, "POST", &path, &cred.subject, &body)?;
                        if code != 200 {
                            error(
                                &mut c,
                                if code == 403 { "42501" } else { "XX000" },
                                &String::from_utf8_lossy(&body),
                            )?;
                            if status == b'T' {
                                status = b'E';
                            }
                            break;
                        }
                        let res: QueryResult =
                            serde_json::from_slice(&body).map_err(io::Error::other)?;
                        rows(&mut c, &res)?;
                        let tag = match stmt {
                            SqlStmt::Begin => {
                                status = b'T';
                                tx_term = st.meta.role_info().2;
                                "BEGIN".into()
                            }
                            SqlStmt::Commit => {
                                status = b'I';
                                "COMMIT".into()
                            }
                            SqlStmt::Rollback => {
                                status = b'I';
                                "ROLLBACK".into()
                            }
                            SqlStmt::Select { .. } => format!("SELECT {}", res.rows.len()),
                            SqlStmt::Insert { .. } => format!("INSERT 0 {}", res.rows_affected),
                            SqlStmt::Update { .. } => format!("UPDATE {}", res.rows_affected),
                            SqlStmt::Delete { .. } => format!("DELETE {}", res.rows_affected),
                            SqlStmt::CreateTable { .. } => "CREATE TABLE".into(),
                            SqlStmt::CreateIndex { .. } => "CREATE INDEX".into(),
                            SqlStmt::DropIndex { .. } => "DROP INDEX".into(),
                            _ => "SHOW".into(),
                        };
                        packet(&mut c, b'C', format!("{tag}\0").as_bytes())?;
                    }
                    ready(&mut c, status)?;
                }
                _ => {
                    if !discard {
                        error(
                            &mut c,
                            "0A000",
                            "only Simple Query is supported; use a simple-query client",
                        )?;
                        discard = true;
                    }
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
    #[test]
    fn splitting() {
        assert_eq!(
            split("BEGIN; INSERT INTO t VALUES ('a;''b'); -- x;\n COMMIT;").len(),
            3
        );
    }
}
