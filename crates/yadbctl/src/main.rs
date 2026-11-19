use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use serde::Deserialize;
use yadb_blob::{BlobId, ErasureScheme, PoolStriper, VolumeConfig, VolumeStore};
use yadb_control::{parse_manifests, ControlPlaneState, DiffOp, Role};
use yadb_db::{QueryResult, SqlValue};
use yadb_io::{FaultConfig, OsFs, SimFs, StorageFs};

#[derive(Parser)]
#[command(
    name = "yadbctl",
    version,
    about = "Declarative control, SQL, S3 & Block CLI for YADB (Yet Another DB & Data Platform)"
)]
struct Cli {
    /// Live `yadbd` HTTP endpoint (e.g. `http://127.0.0.1:8420`). If omitted, uses local state file.
    #[arg(short = 's', long, global = true)]
    server: Option<String>,

    /// Path to offline state file when `--server` is not set
    #[arg(long, default_value = ".yadb-state.json", global = true)]
    state: PathBuf,

    /// Shared secret used to sign `x-yadb-subject`; when set, every request that carries a subject also carries a matching `x-yadb-subject-signature`.
    #[arg(
        long,
        env = "YADB_SUBJECT_SECRET",
        hide_env_values = true,
        global = true
    )]
    subject_secret: Option<String>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Dry-run declarative YAML manifest changes and validate cluster/pool/project integrity
    Plan {
        #[arg(short = 'f', long)]
        file: PathBuf,
    },
    /// Apply declarative YAML manifests to `yadbd` (or local state) and advance generation
    Apply {
        #[arg(short = 'f', long)]
        file: PathBuf,
        #[arg(long, default_value = "yadbctl")]
        subject: String,
        /// Retry-safe key for a live apply (requires --server)
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Resolve a control key; an unused key is permanently fenced
    Resolve {
        #[arg(long)]
        key: String,
        /// Must match the subject used for apply
        #[arg(long, default_value = "yadbctl")]
        subject: String,
        /// Resolve a project-scope record (block UNMAP) instead of a control one.
        /// The subject must be allowed to write that project.
        #[arg(long)]
        project: Option<String>,
        /// Print the daemon response verbatim
        #[arg(long)]
        json: bool,
    },
    /// Show current cluster generation, pools, projects, databases, object stores, and block volumes
    Status,
    /// Show Raft consensus role, term, leader, and commit index of a `yadbd` node
    RaftStatus,
    /// Show distributed VDisk / StoragePool slot placement and online health across nodes
    PoolHealth,
    /// Run a deep CRC32 scrub & self-healing repair pass across all StoragePools
    PoolScrub {
        /// Signed subject used for the daemon's admin gate
        #[arg(long, default_value = "yadbctl")]
        subject: String,
    },
    /// Run a garbage-collection pass across all StoragePools on a live node
    PoolGc {
        /// Signed subject used for the daemon's admin gate
        #[arg(long, default_value = "yadbctl")]
        subject: String,
        /// Print the daemon response verbatim
        #[arg(long)]
        json: bool,
    },
    /// Verify local zero-hop RBAC authorization for a subject on a project
    AuthCheck {
        #[arg(long)]
        project: String,
        #[arg(long)]
        subject: String,
        #[arg(long, default_value_t = false)]
        write: bool,
    },
    /// Execute a SQL query against a sharded project Database on `yadbd`
    Sql {
        #[arg(long)]
        project: String,
        #[arg(long)]
        db: String,
        #[arg(long)]
        subject: String,
        /// SQL statement (`CREATE TABLE`, `INSERT`, `UPSERT`, `SELECT`, `DELETE`, `FLUSH TABLE`, `SHOW TABLES`)
        query: String,
    },
    /// Write raw bytes (from `--data` string or `-f` file) at `--offset` on a project's BlockVolume
    BlockWrite {
        #[arg(long)]
        project: String,
        #[arg(long)]
        volume: String,
        #[arg(long)]
        subject: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        data: Option<String>,
        #[arg(short = 'f', long)]
        file: Option<PathBuf>,
    },
    /// Read `--len` bytes at `--offset` from a project's thin-provisioned BlockVolume
    BlockRead {
        #[arg(long)]
        project: String,
        #[arg(long)]
        volume: String,
        #[arg(long)]
        subject: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long)]
        len: usize,
        #[arg(short = 'o', long)]
        out: Option<PathBuf>,
    },
    /// Upload an object to a project's ObjectStore on a live `yadbd` node
    S3Put {
        #[arg(long)]
        project: String,
        #[arg(long)]
        store: String,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        key: String,
        #[arg(long)]
        subject: String,
        #[arg(short = 'f', long)]
        file: PathBuf,
    },
    /// Download an object (or byte range) from a project's ObjectStore on `yadbd`
    S3Get {
        #[arg(long)]
        project: String,
        #[arg(long)]
        store: String,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        key: String,
        #[arg(long)]
        subject: String,
        /// Optional inclusive byte range, e.g. `0-63`
        #[arg(long)]
        range: Option<String>,
        #[arg(short = 'o', long)]
        out: Option<PathBuf>,
    },
    /// List objects in a project's ObjectStore bucket by prefix
    S3Ls {
        #[arg(long)]
        project: String,
        #[arg(long)]
        store: String,
        #[arg(long)]
        bucket: String,
        #[arg(long)]
        subject: String,
        #[arg(long, default_value = "")]
        prefix: String,
    },
    /// Run deterministic seeded crash & Reed-Solomon self-healing simulation in memory
    SimCrash {
        #[arg(long, default_value_t = 2026)]
        seed: u64,
    },
    /// Benchmark/demo real OS filesystem erasure-coded PoolStriper (RS 4+2) on a directory
    DemoPool {
        #[arg(
            long,
            default_value = "/data/data/com.foxdebug.acodefree/cache/opencode/yadb-demo"
        )]
        dir: PathBuf,
    },
}

#[derive(Deserialize)]
struct RemotePlanResp {
    current_generation: u64,
    next_generation: u64,
    ops: Vec<DiffOp>,
}

#[derive(Deserialize)]
struct RemoteApplyResp {
    generation: u64,
    ops: Vec<DiffOp>,
}

#[derive(Deserialize)]
struct ResolveResponse {
    outcome: String,
    reason: Option<String>,
    response: Option<RecordedResponse>,
}

#[derive(Deserialize)]
struct RecordedResponse {
    status: u16,
    body_base64: String,
}

/// Mirrors `valid_idempotency_key` in `yadbd` (`crates/yadbd/src/main.rs`). The
/// two crates cannot share it, so the rule is duplicated deliberately: rejecting
/// a bad key here gives the operator a precise message instead of a bare 400 from
/// the daemon, and it must never accept something the daemon would refuse.
fn validate_idempotency_key(key: &str) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = key.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 128
        || !bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(b))
    {
        return Err("idempotency key must be 1–128 bytes and contain only ASCII letters, digits, '.', '_', '~', or '-'".into());
    }
    Ok(())
}

fn apply_request(
    srv: &str,
    payload: &[u8],
    subject: &str,
    secret: Option<&str>,
    key: Option<&str>,
) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
    let mut headers = vec![("Content-Type", "application/json")];
    if let Some(key) = key {
        validate_idempotency_key(key)?;
        headers.push(("Idempotency-Key", key));
    }
    http_subject_request(srv, "POST", "/v1/apply", &headers, payload, subject, secret)
}

fn resolve_request(
    srv: &str,
    key: &str,
    subject: &str,
    secret: Option<&str>,
) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
    resolve_request_scoped(srv, key, subject, secret, None)
}

/// Resolve a record in the control scope, or in a project's scope when
/// `project` is given.
///
/// The scope is part of the request because it selects the keyspace *and* the
/// credential: a control resolve is an admin operation, while a project record
/// belongs to whoever may write that project. Sending the wrong one would either
/// fence a key nobody meant to fence or fail authorization.
fn resolve_request_scoped(
    srv: &str,
    key: &str,
    subject: &str,
    secret: Option<&str>,
    project: Option<&str>,
) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
    validate_idempotency_key(key)?;
    let payload = match project {
        Some(project) => serde_json::to_vec(
            &serde_json::json!({"scope": "project", "project": project, "key": key}),
        )?,
        None => serde_json::to_vec(&serde_json::json!({"scope": "control", "key": key}))?,
    };
    http_subject_request(
        srv,
        "POST",
        "/v1/idempotency/resolve",
        &[("Content-Type", "application/json")],
        &payload,
        subject,
        secret,
    )
}

fn resolve_success(code: u16, outcome: &str) -> bool {
    // Only definite answers are success; unknown must not be mistaken for a failed write.
    code == 200 && matches!(outcome, "committed" | "not_committed")
}

/// Which collection handler produced a response body.
///
/// The two bodies are not interchangeable: `/v1/pools/gc` returns
/// `SweepOutcome` with `barriers_applied`, while `/v1/pools/scrub` returns
/// `ScrubSummary`, which carries `refused_puts` and `repair_error` but not
/// `barriers_applied`. A counter absent from the body is printed as `n/a`
/// rather than invented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CollectionCommand {
    Gc,
    Scrub,
}

impl CollectionCommand {
    fn display_name(self) -> &'static str {
        match self {
            CollectionCommand::Gc => "Pool GC",
            CollectionCommand::Scrub => "Pool scrub",
        }
    }

    /// The field that names a refusal. Scrub's repair half reports groups it
    /// could not scan in `repair_error`; GC uses `gc_refusal`. A scrub can also
    /// answer 409 because its collection half refused, so `gc_refusal` is a
    /// fallback there.
    fn refusal_field(self) -> &'static str {
        match self {
            CollectionCommand::Gc => "gc_refusal",
            CollectionCommand::Scrub => "repair_error",
        }
    }
}

/// What an operator should see for one collection response: the process exit
/// code and the exact lines, in order.
#[derive(Debug, PartialEq, Eq)]
struct CollectionReport {
    exit_code: i32,
    lines: Vec<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct CollectionCounters {
    blobs_live: Option<u64>,
    fragments_deleted: Option<u64>,
    barriers_applied: Option<u64>,
    leases_live: Option<u64>,
    refused_puts: Option<u64>,
    pools_refused: Vec<String>,
    groups_refused: Vec<String>,
}

impl CollectionCounters {
    fn line(&self, groups_refused: usize) -> String {
        fn count(value: Option<u64>) -> String {
            value
                .map(|n| n.to_string())
                .unwrap_or_else(|| "n/a".to_string())
        }
        format!(
            "Counters: blobs live {}, fragments deleted {}, barriers applied {}, leases live {}, \
             refused puts {}, pools refused {}, groups refused {}",
            count(self.blobs_live),
            count(self.fragments_deleted),
            count(self.barriers_applied),
            count(self.leases_live),
            count(self.refused_puts),
            self.pools_refused.len(),
            groups_refused,
        )
    }
}

/// Read a collection response into an exit code and printable lines.
///
/// Three daemon shapes matter:
///
/// * `200` - the pass ran; print its counters and exit `0`.
/// * `409` naming groups - this node leads only some metadata groups. The
///   counters cover the groups it did run, so this is a partial pass: list the
///   named groups, say the same command must run on their leaders, exit `2`.
/// * `409` with no named groups and every counter zero - no group ran. When the
///   refusal names the unavailable control-plane snapshot, say so and that the
///   node must reach the control group's quorum; exit `1`.
///
/// Anything else is an error. A malformed body is reported, never guessed at
/// and never allowed to panic.
fn interpret_collection_response(
    command: CollectionCommand,
    status_code: u16,
    body: &[u8],
) -> CollectionReport {
    let raw = String::from_utf8_lossy(body);
    let raw = raw.trim_end();

    if status_code != 200 && status_code != 409 {
        return CollectionReport {
            exit_code: 1,
            lines: vec![format!(
                "{} failed (HTTP {status_code}): {raw}",
                command.display_name()
            )],
        };
    }

    let value: serde_json::Value = match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(value) if value.is_object() => value,
        _ => {
            return CollectionReport {
                exit_code: 1,
                lines: vec![format!(
                    "Malformed {} response (HTTP {status_code}): {raw}",
                    command.display_name()
                )],
            }
        }
    };

    let counters = CollectionCounters {
        blobs_live: value.get("blobs_live").and_then(|v| v.as_u64()),
        fragments_deleted: value.get("fragments_deleted").and_then(|v| v.as_u64()),
        barriers_applied: value.get("barriers_applied").and_then(|v| v.as_u64()),
        leases_live: value.get("leases_live").and_then(|v| v.as_u64()),
        refused_puts: value.get("refused_puts").and_then(|v| v.as_u64()),
        pools_refused: string_array(&value, "gc_pools_refused"),
        groups_refused: string_array(&value, "gc_groups_refused"),
    };

    let mut refusal = value
        .get(command.refusal_field())
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if command == CollectionCommand::Scrub && refusal.is_none() {
        refusal = value
            .get("gc_refusal")
            .and_then(|v| v.as_str())
            .map(str::to_string);
    }

    if status_code == 200 {
        return CollectionReport {
            exit_code: 0,
            lines: vec![
                format!("{} completed.", command.display_name()),
                counters.line(counters.groups_refused.len()),
            ],
        };
    }

    // 409. Named groups mean the pass ran for the groups this node leads; that
    // is a partial result, not a failed pass.
    let mut named = group_ids_in(refusal.as_deref().unwrap_or(""));
    for entry in &counters.groups_refused {
        for id in group_ids_in(entry) {
            if !named.contains(&id) {
                named.push(id);
            }
        }
    }

    if !named.is_empty() {
        let mut lines = vec![
            format!(
                "{} partially completed: {} metadata group(s) were refused.",
                command.display_name(),
                named.len()
            ),
            counters.line(named.len()),
            format!("Refused groups: {}", named.join(", ")),
            "Run the same command on the leader of each named group; the counters above cover only \
             the groups this node leads."
                .to_string(),
        ];
        if let Some(reason) = &refusal {
            lines.push(format!("Daemon refusal: {reason}"));
        }
        return CollectionReport {
            exit_code: 2,
            lines,
        };
    }

    // No group was named. If every counter is zero, nothing ran: this is the
    // whole-pass refusal, distinct from a partial pass.
    let nothing_ran = counters.blobs_live.unwrap_or(0) == 0
        && counters.fragments_deleted.unwrap_or(0) == 0
        && counters.barriers_applied.unwrap_or(0) == 0
        && counters.leases_live.unwrap_or(0) == 0
        && counters.refused_puts.unwrap_or(0) == 0
        && counters.pools_refused.is_empty()
        && counters.groups_refused.is_empty();

    let reason = refusal.as_deref().unwrap_or("no reason given");
    if nothing_ran && reason.contains("control-plane snapshot") {
        return CollectionReport {
            exit_code: 1,
            lines: vec![
                format!(
                    "{}: nothing was scanned or collected.",
                    command.display_name()
                ),
                "The control-plane snapshot the pass needs for its volume-to-pool map is \
                 unavailable; the node must reach the control group's quorum before running this \
                 command again."
                    .to_string(),
                counters.line(0),
                format!("Daemon refusal: {reason}"),
            ],
        };
    }

    if nothing_ran {
        return CollectionReport {
            exit_code: 1,
            lines: vec![
                format!(
                    "{}: nothing was scanned or collected.",
                    command.display_name()
                ),
                format!("Daemon refusal: {reason}"),
                counters.line(0),
            ],
        };
    }

    // Counters moved but the daemon named no group. Do not report success: the
    // pass refused something, and the counters cover only what did run.
    CollectionReport {
        exit_code: 2,
        lines: vec![
            format!("{} partially completed: the daemon refused part of the pass.", command.display_name()),
            counters.line(counters.groups_refused.len()),
            "The daemon did not name the refused groups; inspect its body before assuming the pass \
             is complete."
                .to_string(),
            format!("Daemon refusal: {reason}"),
        ],
    }
}

fn string_array(value: &serde_json::Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Group ids in text the daemon writes as `group {id} refused: ...`.
fn group_ids_in(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i + 6 <= bytes.len() {
        if &bytes[i..i + 6] == b"group " {
            let start = i + 6;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if end > start && bytes[end..].starts_with(b" refused") {
                let id = text[start..end].to_string();
                if !out.contains(&id) {
                    out.push(id);
                }
            }
            i = end.max(start);
        } else {
            i += 1;
        }
    }
    out
}

fn print_collection_report(report: &CollectionReport) {
    for line in &report.lines {
        println!("{line}");
    }
    if report.exit_code != 0 {
        std::process::exit(report.exit_code);
    }
}

fn decode_base64(input: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    fn value(b: u8) -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes = input.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err("invalid recorded base64".into());
    }
    let mut out = Vec::new();
    for (i, chunk) in bytes.chunks_exact(4).enumerate() {
        let a = value(chunk[0]).ok_or("invalid recorded base64")?;
        let b = value(chunk[1]).ok_or("invalid recorded base64")?;
        let c = if chunk[2] == b'=' {
            0
        } else {
            value(chunk[2]).ok_or("invalid recorded base64")?
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            value(chunk[3]).ok_or("invalid recorded base64")?
        };
        if (chunk[2] == b'=' && (chunk[3] != b'=' || b & 15 != 0))
            || (chunk[3] == b'=' && chunk[2] != b'=' && c & 3 != 0)
            || ((chunk[2] == b'=' || chunk[3] == b'=') && i + 1 != bytes.len() / 4)
        {
            return Err("invalid recorded base64".into());
        }
        out.push((a << 2) | (b >> 4));
        if chunk[2] != b'=' {
            out.push((b << 4) | (c >> 2));
        }
        if chunk[3] != b'=' {
            out.push((c << 6) | d);
        }
    }
    Ok(out)
}

fn subject_signature(secret: &str, subject: &str) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    ring::hmac::sign(&key, subject.as_bytes())
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn subject_headers(subject: &str, secret: Option<&str>) -> Vec<(String, String)> {
    let mut headers = vec![("x-yadb-subject".to_string(), subject.to_string())];
    if !subject.is_empty() {
        if let Some(secret) = secret {
            headers.push((
                "x-yadb-subject-signature".to_string(),
                subject_signature(secret, subject),
            ));
        }
    }
    headers
}

fn http_subject_request(
    base_url: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    subject: &str,
    secret: Option<&str>,
) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
    let subject_headers = subject_headers(subject, secret);
    let mut headers = headers.to_vec();
    headers.extend(
        subject_headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str())),
    );
    http_request(base_url, method, path, &headers, body)
}

/// Zero-dependency HTTP/1.1 client over `TcpStream` (`Connection: close`) for fast `yadbd` calls.
fn http_request(
    base_url: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<(u16, Vec<u8>), Box<dyn std::error::Error>> {
    let addr = base_url
        .trim()
        .strip_prefix("http://")
        .unwrap_or(base_url.trim())
        .trim_end_matches('/');

    let mut stream = TcpStream::connect(addr)?;
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");

    stream.write_all(req.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;

    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("malformed HTTP response from yadbd")?;
    let head_str = std::str::from_utf8(&raw[..split])?;
    let status_line = head_str.lines().next().ok_or("empty HTTP status line")?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("missing HTTP status code")?
        .parse()?;

    let payload = &raw[split + 4..];
    let is_chunked = head_str.lines().any(|l| {
        l.to_ascii_lowercase().starts_with("transfer-encoding:")
            && l.to_ascii_lowercase().contains("chunked")
    });
    let decoded = if is_chunked {
        decode_chunked(payload)?
    } else {
        payload.to_vec()
    };

    Ok((status, decoded))
}

fn decode_chunked(mut src: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    loop {
        let crlf = src
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("invalid chunked framing")?;
        let len_hex = std::str::from_utf8(&src[..crlf])?.trim();
        let chunk_len = usize::from_str_radix(len_hex, 16)?;
        src = &src[crlf + 2..];
        if chunk_len == 0 {
            break;
        }
        if src.len() < chunk_len + 2 {
            return Err("truncated HTTP chunk".into());
        }
        out.extend_from_slice(&src[..chunk_len]);
        src = &src[chunk_len + 2..];
    }
    Ok(out)
}

fn load_state(path: &PathBuf) -> Result<ControlPlaneState, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Ok(ControlPlaneState::new());
    }
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn save_state(path: &PathBuf, state: &ControlPlaneState) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string_pretty(state)?;
    fs::write(path, json)?;
    Ok(())
}

fn print_ops(ops: &[DiffOp]) {
    for op in ops {
        match op {
            DiffOp::Create { key } => println!("  + create    {key}"),
            DiffOp::Update { key } => println!("  ~ update    {key}"),
            DiffOp::Unchanged { key } => println!("  = unchanged {key}"),
        }
    }
}

fn print_status(state: &ControlPlaneState) {
    println!("Cluster Generation : {}", state.generation);
    if let Some((meta, spec)) = &state.cluster {
        println!(
            "Cluster            : {} ({} failure domains)",
            meta.name,
            spec.domains.len()
        );
    } else {
        println!("Cluster            : <unconfigured>");
    }
    println!("Storage Pools ({}):", state.pools.len());
    for (name, p) in &state.pools {
        println!(
            "  - {name}: {:?} across {} failure domains",
            p.erasure, p.failure_domains
        );
    }
    println!("Projects ({}):", state.projects.len());
    for (name, (pid, spec)) in &state.projects {
        println!(
            "  - {name} (project_id={pid}, members={}, max_iops={:?})",
            spec.members.len(),
            spec.quotas.max_iops
        );
    }
    println!("Databases ({}):", state.databases.len());
    for (full_key, db) in &state.databases {
        println!(
            "  - {full_key}: wal={}, data={}, isolation={:?}, shards={}",
            db.wal_pool, db.data_pool, db.isolation, db.shards
        );
    }
    println!("ObjectStores ({}):", state.object_stores.len());
    for (full_key, os) in &state.object_stores {
        println!(
            "  - {full_key}: pool={}, isolation={:?}, buckets={:?}",
            os.pool, os.isolation, os.buckets
        );
    }
    println!("BlockVolumes ({}):", state.block_volumes.len());
    for (full_key, bv) in &state.block_volumes {
        println!(
            "  - {full_key}: pool={}, size_bytes={}",
            bv.pool, bv.size_bytes
        );
    }
}

fn format_sql_val(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".into(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Int(n) => n.to_string(),
        SqlValue::Text(s) => s.clone(),
    }
}

fn print_query_result(qr: &QueryResult) {
    if qr.columns.is_empty() {
        println!("OK (rows_affected={})", qr.rows_affected);
        return;
    }

    let mut widths: Vec<usize> = qr.columns.iter().map(|c| c.len()).collect();
    let rendered: Vec<Vec<String>> = qr
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, cell)| {
                    let s = format_sql_val(cell);
                    if i < widths.len() {
                        widths[i] = widths[i].max(s.len());
                    }
                    s
                })
                .collect()
        })
        .collect();

    let hdr: Vec<String> = qr
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:<width$}", c, width = widths[i]))
        .collect();
    let sep: Vec<String> = widths.iter().map(|&w| "-".repeat(w)).collect();

    println!("{}", hdr.join(" | "));
    println!("{}", sep.join("-+-"));
    for r in &rendered {
        let line: Vec<String> = r
            .iter()
            .enumerate()
            .map(|(i, cell)| format!("{:<width$}", cell, width = widths[i]))
            .collect();
        println!("{}", line.join(" | "));
    }
    println!("({} rows)", rendered.len());
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.cmd {
        Command::Plan { file } => {
            let yaml = fs::read_to_string(&file)?;
            if let Some(srv) = &cli.server {
                let payload = serde_json::to_vec(&serde_json::json!({ "yaml": yaml }))?;
                let (code, body) = http_request(
                    srv,
                    "POST",
                    "/v1/plan",
                    &[("Content-Type", "application/json")],
                    &payload,
                )?;
                if code != 200 {
                    eprintln!("Server error ({code}): {}", String::from_utf8_lossy(&body));
                    std::process::exit(1);
                }
                let resp: RemotePlanResp = serde_json::from_slice(&body)?;
                println!(
                    "Remote Plan OK (current generation: {}, next generation: {}):",
                    resp.current_generation, resp.next_generation
                );
                print_ops(&resp.ops);
            } else {
                let manifests = parse_manifests(&yaml)?;
                let state = load_state(&cli.state)?;
                let (ops, next) = state.plan(&manifests)?;
                println!(
                    "Plan OK (current generation: {}, next generation: {}):",
                    state.generation, next.generation
                );
                print_ops(&ops);
            }
        }
        Command::Apply {
            file,
            subject,
            idempotency_key,
        } => {
            if let Some(key) = &idempotency_key {
                validate_idempotency_key(key)?;
                if cli.server.is_none() {
                    return Err("--idempotency-key requires --server: local apply has no replicated idempotency record".into());
                }
            }
            let yaml = fs::read_to_string(&file)?;
            if let Some(srv) = &cli.server {
                let payload = serde_json::to_vec(&serde_json::json!({ "yaml": yaml }))?;
                let (code, body) = apply_request(
                    srv,
                    &payload,
                    &subject,
                    cli.subject_secret.as_deref(),
                    idempotency_key.as_deref(),
                )?;
                if code != 200 {
                    eprintln!("Apply failed ({code}): {}", String::from_utf8_lossy(&body));
                    std::process::exit(1);
                }
                let resp: RemoteApplyResp = serde_json::from_slice(&body)?;
                println!("Applied to yadbd -> generation {}", resp.generation);
                print_ops(&resp.ops);
            } else {
                let manifests = parse_manifests(&yaml)?;
                let mut state = load_state(&cli.state)?;
                let ops = state.apply(&manifests)?;
                save_state(&cli.state, &state)?;
                println!("Applied locally -> generation {}", state.generation);
                print_ops(&ops);
            }
        }
        Command::Resolve {
            key,
            subject,
            project,
            json,
        } => {
            let srv = cli.server.as_deref().ok_or(
                "resolve requires --server; resolving an unused key permanently fences it",
            )?;
            let (code, body) = resolve_request_scoped(
                srv,
                &key,
                &subject,
                cli.subject_secret.as_deref(),
                project.as_deref(),
            )?;
            if code != 200 && code != 503 {
                eprintln!(
                    "Resolve failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                );
                std::process::exit(1);
            }
            let resp: ResolveResponse = serde_json::from_slice(&body).map_err(|_| {
                format!(
                    "Resolve failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                )
            })?;
            if json {
                std::io::stdout().write_all(&body)?;
            } else {
                println!("Outcome: {}", resp.outcome);
                match resp.outcome.as_str() {
                    "committed" => {
                        let recorded = resp
                            .response
                            .as_ref()
                            .ok_or("committed resolve response missing recorded response")?;
                        let decoded = decode_base64(&recorded.body_base64)?;
                        println!("Recorded HTTP status: {}", recorded.status);
                        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&decoded) {
                            println!("{}", serde_json::to_string_pretty(&value)?);
                        } else {
                            let preview = &decoded[..decoded.len().min(128)];
                            println!(
                                "Recorded body: {} bytes; first bytes: {:?}",
                                decoded.len(),
                                preview
                            );
                        }
                    }
                    "not_committed" => {
                        println!("Reason: {}", resp.reason.as_deref().unwrap_or("unknown"));
                        println!("This key is permanently fenced; a later apply with it will be refused.");
                    }
                    "unknown" => {
                        println!("Reason: {}", resp.reason.as_deref().unwrap_or("unknown"));
                        println!("This is NOT proof that the write did not happen.");
                    }
                    _ => return Err("unrecognized resolve outcome".into()),
                }
            }
            if !resolve_success(code, &resp.outcome) {
                std::process::exit(1);
            }
        }
        Command::Status => {
            let state = if let Some(srv) = &cli.server {
                let (code, body) = http_request(srv, "GET", "/v1/status", &[], &[])?;
                if code != 200 {
                    eprintln!("Status failed ({code}): {}", String::from_utf8_lossy(&body));
                    std::process::exit(1);
                }
                serde_json::from_slice(&body)?
            } else {
                load_state(&cli.state)?
            };
            print_status(&state);
        }
        Command::RaftStatus => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let (code, body) = http_request(srv, "GET", "/raft/status", &[], &[])?;
            if code != 200 {
                eprintln!(
                    "Raft status failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                );
                std::process::exit(1);
            }
            let val: serde_json::Value = serde_json::from_slice(&body)?;
            println!("{}", serde_json::to_string_pretty(&val)?);
        }
        Command::PoolHealth => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let (code, body) = http_request(srv, "GET", "/v1/pools/health", &[], &[])?;
            if code != 200 {
                eprintln!(
                    "Pool health failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                );
                std::process::exit(1);
            }
            let val: serde_json::Value = serde_json::from_slice(&body)?;
            println!("{}", serde_json::to_string_pretty(&val)?);
        }
        Command::PoolScrub { subject } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let (code, body) = http_subject_request(
                srv,
                "POST",
                "/v1/pools/scrub",
                &[],
                &[],
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            let report = interpret_collection_response(CollectionCommand::Scrub, code, &body);
            print_collection_report(&report);
        }
        Command::PoolGc { subject, json } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let (code, body) = http_subject_request(
                srv,
                "POST",
                "/v1/pools/gc",
                &[],
                &[],
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            let report = interpret_collection_response(CollectionCommand::Gc, code, &body);
            if json {
                std::io::stdout().write_all(&body)?;
            } else {
                print_collection_report(&report);
            }
            if report.exit_code != 0 {
                std::process::exit(report.exit_code);
            }
        }
        Command::AuthCheck {
            project,
            subject,
            write,
        } => {
            let state = if let Some(srv) = &cli.server {
                let (_, body) = http_request(srv, "GET", "/v1/status", &[], &[])?;
                serde_json::from_slice(&body)?
            } else {
                load_state(&cli.state)?
            };
            let role = if write {
                Role::ReadWrite
            } else {
                Role::ReadOnly
            };
            match state.authorize(&project, &subject, role) {
                Ok(pid) => println!(
                    "ALLOW: `{subject}` authorized on project `{project}` (project_id={pid}, mode={role:?})"
                ),
                Err(e) => {
                    eprintln!("DENY: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::Sql {
            project,
            db,
            subject,
            query,
        } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let payload = serde_json::to_vec(&serde_json::json!({ "sql": query }))?;
            let path = format!("/db/{project}/{db}/query");
            let (code, body) = http_subject_request(
                srv,
                "POST",
                &path,
                &[("Content-Type", "application/json")],
                &payload,
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            if code != 200 {
                eprintln!("SQL failed ({code}): {}", String::from_utf8_lossy(&body));
                std::process::exit(1);
            }
            let qr: QueryResult = serde_json::from_slice(&body)?;
            print_query_result(&qr);
        }
        Command::BlockWrite {
            project,
            volume,
            subject,
            offset,
            data,
            file,
        } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let bytes = if let Some(p) = file {
                fs::read(p)?
            } else if let Some(s) = data {
                s.into_bytes()
            } else {
                return Err("specify either `--data <str>` or `-f <file>`".into());
            };
            let path = format!("/block/{project}/{volume}?offset={offset}");
            let (code, body) = http_subject_request(
                srv,
                "PUT",
                &path,
                &[],
                &bytes,
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            if code != 200 {
                eprintln!(
                    "Block write failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                );
                std::process::exit(1);
            }
            println!("Block WRITE OK: {}", String::from_utf8_lossy(&body));
        }
        Command::BlockRead {
            project,
            volume,
            subject,
            offset,
            len,
            out,
        } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let path = format!("/block/{project}/{volume}?offset={offset}&len={len}");
            let (code, body) = http_subject_request(
                srv,
                "GET",
                &path,
                &[],
                &[],
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            if code != 200 {
                eprintln!(
                    "Block read failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                );
                std::process::exit(1);
            }
            if let Some(dst) = out {
                fs::write(&dst, &body)?;
                println!("Saved {} bytes from BlockVolume to {:?}", body.len(), dst);
            } else {
                std::io::stdout().write_all(&body)?;
                println!();
            }
        }
        Command::S3Put {
            project,
            store,
            bucket,
            key,
            subject,
            file,
        } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let data = fs::read(&file)?;
            let path = format!(
                "/s3/{project}/{store}/{bucket}/{}",
                key.trim_start_matches('/')
            );
            let (code, body) = http_subject_request(
                srv,
                "PUT",
                &path,
                &[],
                &data,
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            if code != 200 {
                eprintln!(
                    "S3 PUT denied/failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                );
                std::process::exit(1);
            }
            println!("S3 PUT OK: {}", String::from_utf8_lossy(&body));
        }
        Command::S3Get {
            project,
            store,
            bucket,
            key,
            subject,
            range,
            out,
        } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let path = format!(
                "/s3/{project}/{store}/{bucket}/{}",
                key.trim_start_matches('/')
            );
            let mut hdrs = Vec::new();
            let range_val;
            if let Some(r) = &range {
                range_val = format!("bytes={r}");
                hdrs.push(("Range", range_val.as_str()));
            }
            let (code, body) = http_subject_request(
                srv,
                "GET",
                &path,
                &hdrs,
                &[],
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            if code != 200 && code != 206 {
                eprintln!("S3 GET failed ({code}): {}", String::from_utf8_lossy(&body));
                std::process::exit(1);
            }
            if let Some(dst) = out {
                fs::write(&dst, &body)?;
                println!("Saved {} bytes (HTTP {code}) to {:?}", body.len(), dst);
            } else {
                std::io::stdout().write_all(&body)?;
                println!();
            }
        }
        Command::S3Ls {
            project,
            store,
            bucket,
            subject,
            prefix,
        } => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let path = format!("/s3/{project}/{store}/{bucket}?prefix={prefix}");
            let (code, body) = http_subject_request(
                srv,
                "GET",
                &path,
                &[],
                &[],
                &subject,
                cli.subject_secret.as_deref(),
            )?;
            if code != 200 {
                eprintln!("S3 LS failed ({code}): {}", String::from_utf8_lossy(&body));
                std::process::exit(1);
            }
            let pretty: serde_json::Value = serde_json::from_slice(&body)?;
            println!("{}", serde_json::to_string_pretty(&pretty)?);
        }
        Command::SimCrash { seed } => {
            println!(
                "Running deterministic crash & erasure self-healing simulation (seed={seed})..."
            );
            let fs = SimFs::new(seed);
            let mut domains = Vec::new();
            for d in 0..6 {
                domains.push(VolumeStore::open(
                    fs.clone(),
                    format!("domain-{d}"),
                    VolumeConfig::default(),
                )?);
            }
            let striper = PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, domains)?;

            for step in 1..=20u32 {
                let id = BlobId::new(1, 10, 1, step, 2, 0);
                let payload = format!("tablet-row-block-{step:04}-payload-verified").into_bytes();
                striper.put_blob(id, &payload)?;
            }

            fs.set_faults(FaultConfig {
                seed: seed ^ 0xDEAD_BEEF,
                torn_write_prob: 1.0,
                read_eio_prob: 0.0,
                write_eio_prob: 0.0,
                ..Default::default()
            });
            let _ = striper.domains()[0].put(BlobId::new(1, 10, 1, 999, 2, 1), b"torn-tail-bytes");
            fs.set_faults(FaultConfig::default());
            fs.simulate_power_cut();

            for f in fs.list_files("domain-2")? {
                fs.remove_file(&format!("domain-2/{f}"))?;
            }
            if let Some(f0) = fs.list_files("domain-0")?.first() {
                fs.corrupt_byte(&format!("domain-0/{f0}"), 48)?;
            }

            let mut reopened_domains = Vec::new();
            for d in 0..6 {
                reopened_domains.push(VolumeStore::open(
                    fs.clone(),
                    format!("domain-{d}"),
                    VolumeConfig::default(),
                )?);
            }
            let recovered_striper =
                PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, reopened_domains)?;

            let mut total_repaired = 0usize;
            for step in 1..=20u32 {
                let id = BlobId::new(1, 10, 1, step, 2, 0);
                let expected = format!("tablet-row-block-{step:04}-payload-verified").into_bytes();
                let data = recovered_striper.get_blob(id)?;
                assert_eq!(data, expected);
                let rep = recovered_striper.repair_blob(id)?;
                total_repaired += rep.repaired_parts;
            }
            println!(
                "PASS: 20/20 erasure-coded blobs survived power cut + torn write + domain loss (self-healed {total_repaired} fragments)."
            );
        }
        Command::DemoPool { dir } => {
            if dir.exists() {
                fs::remove_dir_all(&dir)?;
            }
            let os_fs = OsFs::new(&dir)?;
            let mut domains = Vec::new();
            for i in 0..6 {
                domains.push(VolumeStore::open(
                    os_fs.clone(),
                    format!("mount_{i}"),
                    VolumeConfig::default(),
                )?);
            }
            let pool = PoolStriper::new(ErasureScheme::ReedSolomon { k: 4, m: 2 }, domains)?;
            let start = std::time::Instant::now();
            let block = vec![0xABu8; 64 * 1024];
            let count = 64u32;
            for step in 1..=count {
                let id = BlobId::new(1, 42, 1, step, 2, 0);
                pool.put_blob(id, &block)?;
            }
            let elapsed = start.elapsed();
            let sample = pool.get_blob(BlobId::new(1, 42, 1, 1, 2, 0))?;
            assert_eq!(sample.len(), block.len());
            println!(
                "OS FS Demo OK at {:?}: wrote & synced {} x 64KiB RS(4+2) blobs across 6 volume mounts in {:.2?}",
                dir, count, elapsed
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        apply_request, decode_base64, interpret_collection_response, resolve_request,
        resolve_request_scoped, resolve_success, subject_headers, subject_signature,
        validate_idempotency_key, CollectionCommand, CollectionReport,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn capture_request(f: impl FnOnce(&str)) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 1024];
            loop {
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buf[..n]);
                if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let header = std::str::from_utf8(&request[..end]).unwrap();
                    let len: usize = header
                        .lines()
                        .find_map(|l| l.strip_prefix("Content-Length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if request.len() >= end + 4 + len {
                        break;
                    }
                }
            }
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        f(&format!("http://{addr}"));
        thread.join().unwrap()
    }

    #[test]
    fn idempotency_key_validation_boundaries() {
        for key in ["a", "-", "_", "~", ".", "AZaz09._~-", &"a".repeat(128)] {
            assert!(validate_idempotency_key(key).is_ok(), "{key:?}");
        }
        for key in ["", &"a".repeat(129), "a b", "/", "é", "\n"] {
            assert!(validate_idempotency_key(key).is_err(), "{key:?}");
        }
        for byte in 0..=127u8 {
            let key = String::from_utf8(vec![byte]).unwrap();
            assert_eq!(
                validate_idempotency_key(&key).is_ok(),
                byte.is_ascii_alphanumeric() || b"._~-".contains(&byte)
            );
        }
    }

    #[test]
    fn apply_sends_optional_idempotency_header() {
        for key in [Some("safe-key"), None] {
            let request = capture_request(|srv| {
                apply_request(srv, b"{}", "alice", Some("secret"), key).unwrap();
            });
            assert!(request.starts_with("POST /v1/apply HTTP/1.1\r\n"));
            assert_eq!(
                request.contains("Idempotency-Key: safe-key\r\n"),
                key.is_some()
            );
            assert!(request.contains("x-yadb-subject-signature: "));
        }
    }

    #[test]
    fn resolve_sends_control_key_and_signed_subject() {
        let request = capture_request(|srv| {
            resolve_request(srv, "key-1", "alice", Some("secret")).unwrap();
        });
        assert!(request.starts_with("POST /v1/idempotency/resolve HTTP/1.1\r\n"));
        assert!(request.contains("x-yadb-subject: alice\r\n"));
        assert!(request.contains(&format!(
            "x-yadb-subject-signature: {}\r\n",
            subject_signature("secret", "alice")
        )));
        let body = request.split_once("\r\n\r\n").unwrap().1;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            serde_json::json!({"scope":"control", "key":"key-1"})
        );
    }

    #[test]
    fn resolve_sends_project_scope_when_a_project_is_named() {
        // The scope selects both the keyspace and the credential, so the CLI must
        // never send a control resolve for a project key (that would fence a
        // control record nobody meant to fence) or omit the project name.
        let request = capture_request(|srv| {
            resolve_request_scoped(srv, "key-1", "alice", Some("secret"), Some("billing")).unwrap();
        });
        let body = request.split_once("\r\n\r\n").unwrap().1;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            serde_json::json!({"scope":"project", "project":"billing", "key":"key-1"})
        );
        // Without a project the payload stays byte-identical to the control form.
        let request = capture_request(|srv| {
            resolve_request_scoped(srv, "key-1", "alice", Some("secret"), None).unwrap();
        });
        let body = request.split_once("\r\n\r\n").unwrap().1;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(body).unwrap(),
            serde_json::json!({"scope":"control", "key":"key-1"})
        );
    }

    #[test]
    fn resolve_exit_status_requires_definite_success() {
        assert!(resolve_success(200, "committed"));
        assert!(resolve_success(200, "not_committed"));
        assert!(!resolve_success(503, "unknown"));
        assert!(!resolve_success(200, "unknown"));
        assert!(!resolve_success(503, "committed"));
    }

    #[test]
    fn recorded_base64_decoding() {
        for (encoded, expected) in [
            ("", b"".as_slice()),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("//8=", b"\xff\xff"),
        ] {
            assert_eq!(decode_base64(encoded).unwrap(), expected);
        }
        for encoded in ["a", "====", "Zg=A", "Zh==", "Zm9=", "Zg==AAAA"] {
            assert!(decode_base64(encoded).is_err());
        }
    }

    #[test]
    fn signature_matches_known_hmac_sha256_vector() {
        assert_eq!(
            subject_signature("key", "The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn signature_is_lowercase_hex() {
        let signature = subject_signature("key", "subject");
        assert_eq!(signature.len(), 64);
        assert!(signature
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
    }

    #[test]
    fn headers_include_exactly_one_signature_only_with_secret() {
        let signed = subject_headers("alice", Some("key"));
        let signatures: Vec<_> = signed
            .iter()
            .filter(|(name, _)| name == "x-yadb-subject-signature")
            .collect();
        assert_eq!(signatures.len(), 1);
        assert_eq!(signatures[0].1, subject_signature("key", "alice"));
        assert!(!subject_headers("alice", None)
            .iter()
            .any(|(name, _)| name == "x-yadb-subject-signature"));
    }

    #[test]
    fn empty_subject_has_no_signature() {
        let headers = subject_headers("", Some("key"));
        assert_eq!(headers, vec![("x-yadb-subject".into(), "".into())]);
    }

    fn collection_body(value: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&value).unwrap()
    }

    fn gc_body(
        blobs_live: u64,
        fragments_deleted: u64,
        barriers_applied: u64,
        leases_live: u64,
        refusal: Option<&str>,
        groups_refused: &[&str],
    ) -> Vec<u8> {
        collection_body(serde_json::json!({
            "blobs_live": blobs_live,
            "fragments_deleted": fragments_deleted,
            "bytes_reclaimed": 4096,
            "leases_live": leases_live,
            "barriers_applied": barriers_applied,
            "gc_refusal": refusal,
            "gc_pools_refused": [],
            "gc_groups_refused": groups_refused,
            "gc_errors": [],
        }))
    }

    fn scrub_body(
        blobs_live: u64,
        fragments_deleted: u64,
        leases_live: u64,
        refused_puts: u64,
        repair_error: Option<&str>,
        groups_refused: &[&str],
    ) -> Vec<u8> {
        collection_body(serde_json::json!({
            "pools_checked": 1,
            "blobs_checked": blobs_live,
            "blobs_intact": blobs_live,
            "blobs_repaired": 0,
            "fragments_repaired": 0,
            "blobs_unrecoverable": 0,
            "refused_puts": refused_puts,
            "degraded_puts": 0,
            "orphan_blobs_collected": 0,
            "orphan_collection_error": null,
            "repair_error": repair_error,
            "blobs_live": blobs_live,
            "fragments_deleted": fragments_deleted,
            "bytes_reclaimed": 0,
            "leases_live": leases_live,
            "gc_refusal": null,
            "gc_pools_refused": [],
            "gc_groups_refused": groups_refused,
            "gc_errors": [],
            "pools": [],
        }))
    }

    fn assert_report(report: &CollectionReport, exit_code: i32, expected: &[&str]) {
        assert_eq!(report.exit_code, exit_code, "{report:?}");
        let expected: Vec<String> = expected.iter().map(|line| line.to_string()).collect();
        assert_eq!(report.lines, expected, "{report:?}");
    }

    #[test]
    fn collection_gc_success_prints_counters_and_exits_zero() {
        let body = gc_body(7, 3, 2, 0, None, &[]);
        let report = interpret_collection_response(CollectionCommand::Gc, 200, &body);
        assert_report(
            &report,
            0,
            &[
                "Pool GC completed.",
                "Counters: blobs live 7, fragments deleted 3, barriers applied 2, leases live 0, refused puts n/a, pools refused 0, groups refused 0",
            ],
        );
    }

    #[test]
    fn collection_scrub_success_prints_counters_and_exits_zero() {
        let body = scrub_body(5, 0, 0, 1, None, &[]);
        let report = interpret_collection_response(CollectionCommand::Scrub, 200, &body);
        assert_report(
            &report,
            0,
            &[
                "Pool scrub completed.",
                "Counters: blobs live 5, fragments deleted 0, barriers applied n/a, leases live 0, refused puts 1, pools refused 0, groups refused 0",
            ],
        );
    }

    #[test]
    fn collection_gc_group_refusal_is_a_partial_pass_exit_two() {
        let refusal = "garbage collection refused for 1 of 2 metadata group(s): group 5 refused: node 2 is not the leader of metadata group 5 (leader is Some(1)); a lagging view could delete a fragment the committed state still references";
        let body = gc_body(4, 2, 1, 0, Some(refusal), &[refusal]);
        let report = interpret_collection_response(CollectionCommand::Gc, 409, &body);
        assert_eq!(report.exit_code, 2, "{report:?}");
        assert_eq!(
            report.lines[1],
            "Counters: blobs live 4, fragments deleted 2, barriers applied 1, leases live 0, refused puts n/a, pools refused 0, groups refused 1"
        );
        assert_eq!(report.lines[2], "Refused groups: 5");
        assert!(report.lines[3].contains("leader of each named group"));
        assert!(report.lines.iter().any(|line| line.contains(refusal)));
    }

    #[test]
    fn collection_scrub_group_refusal_is_a_partial_pass_exit_two() {
        let refusal = "repair scan refused for 1 of 2 metadata group(s): group 5 refused: node 2 is not the leader of metadata group 5";
        let body = scrub_body(4, 1, 0, 0, Some(refusal), &[refusal]);
        let report = interpret_collection_response(CollectionCommand::Scrub, 409, &body);
        assert_eq!(report.exit_code, 2, "{report:?}");
        assert_eq!(
            report.lines[1],
            "Counters: blobs live 4, fragments deleted 1, barriers applied n/a, leases live 0, refused puts 0, pools refused 0, groups refused 1"
        );
        assert_eq!(report.lines[2], "Refused groups: 5");
        assert!(report.lines[3].contains("leader of each named group"));
    }

    #[test]
    fn collection_gc_control_snapshot_refusal_is_a_failed_pass_exit_one() {
        let refusal = "collection refused: the control-plane snapshot a pass needs for its volume-to-pool map is unavailable (control snapshot unavailable); nothing was scanned or collected";
        let body = gc_body(0, 0, 0, 0, Some(refusal), &[]);
        let report = interpret_collection_response(CollectionCommand::Gc, 409, &body);
        assert_eq!(report.exit_code, 1, "{report:?}");
        assert_eq!(
            report.lines[0],
            "Pool GC: nothing was scanned or collected."
        );
        assert!(report.lines[1].contains("control group's quorum"));
        assert_eq!(
            report.lines[2],
            "Counters: blobs live 0, fragments deleted 0, barriers applied 0, leases live 0, refused puts n/a, pools refused 0, groups refused 0"
        );
    }

    #[test]
    fn collection_scrub_control_snapshot_refusal_is_a_failed_pass_exit_one() {
        let refusal = "repair scan refused: the control-plane snapshot a pass needs for its volume-to-pool map is unavailable (control snapshot unavailable); nothing was scanned or collected";
        let body = scrub_body(0, 0, 0, 0, Some(refusal), &[]);
        let report = interpret_collection_response(CollectionCommand::Scrub, 409, &body);
        assert_eq!(report.exit_code, 1, "{report:?}");
        assert_eq!(
            report.lines[0],
            "Pool scrub: nothing was scanned or collected."
        );
        assert!(report.lines[1].contains("control group's quorum"));
        assert_eq!(
            report.lines[2],
            "Counters: blobs live 0, fragments deleted 0, barriers applied n/a, leases live 0, refused puts 0, pools refused 0, groups refused 0"
        );
    }

    #[test]
    fn collection_malformed_body_is_reported_and_never_panics() {
        for command in [CollectionCommand::Gc, CollectionCommand::Scrub] {
            for status in [200u16, 409] {
                for body in [
                    b"not json".as_slice(),
                    b"[1, 2, 3]".as_slice(),
                    b"{".as_slice(),
                    b"".as_slice(),
                ] {
                    let report = interpret_collection_response(command, status, body);
                    assert_eq!(report.exit_code, 1, "{command:?} {status} {report:?}");
                    assert_eq!(report.lines.len(), 1, "{report:?}");
                    assert!(report.lines[0].starts_with("Malformed "), "{report:?}");
                }
            }
        }
    }

    #[test]
    fn collection_unexpected_status_is_an_error() {
        let body = b"{\"error\":\"not an admin\"}";
        for status in [401u16, 403, 500, 503] {
            let report = interpret_collection_response(CollectionCommand::Gc, status, body);
            assert_eq!(report.exit_code, 1, "{report:?}");
            assert!(report.lines[0].contains(&format!("HTTP {status}")));
            assert!(report.lines[0].contains("not an admin"));
        }
    }
}
