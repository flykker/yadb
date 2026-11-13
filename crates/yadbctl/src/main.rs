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
    },
    /// Show current cluster generation, pools, projects, databases, object stores, and block volumes
    Status,
    /// Show Raft consensus role, term, leader, and commit index of a `yadbd` node
    RaftStatus,
    /// Trigger a Raft election round on the target `yadbd` node
    RaftElect,
    /// Show distributed VDisk / StoragePool slot placement and online health across nodes
    PoolHealth,
    /// Run a deep CRC32 scrub & self-healing repair pass across all StoragePools
    PoolScrub,
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
        #[arg(long, default_value = "/data/data/com.foxdebug.acodefree/cache/opencode/yadb-demo")]
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
        Command::Apply { file } => {
            let yaml = fs::read_to_string(&file)?;
            if let Some(srv) = &cli.server {
                let payload = serde_json::to_vec(&serde_json::json!({ "yaml": yaml }))?;
                let (code, body) = http_request(
                    srv,
                    "POST",
                    "/v1/apply",
                    &[("Content-Type", "application/json")],
                    &payload,
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
        Command::RaftElect => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let (code, body) = http_request(srv, "POST", "/raft/elect", &[], &[])?;
            if code != 200 {
                eprintln!(
                    "Raft elect failed ({code}): {}",
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
        Command::PoolScrub => {
            let srv = cli.server.as_deref().unwrap_or("http://127.0.0.1:8420");
            let (code, body) = http_request(srv, "POST", "/v1/pools/scrub", &[], &[])?;
            if code != 200 {
                eprintln!(
                    "Pool scrub failed ({code}): {}",
                    String::from_utf8_lossy(&body)
                );
                std::process::exit(1);
            }
            let val: serde_json::Value = serde_json::from_slice(&body)?;
            println!("{}", serde_json::to_string_pretty(&val)?);
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
            let role = if write { Role::ReadWrite } else { Role::ReadOnly };
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
            let (code, body) = http_request(
                srv,
                "POST",
                &path,
                &[
                    ("Content-Type", "application/json"),
                    ("x-yadb-subject", &subject),
                ],
                &payload,
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
            let (code, body) = http_request(
                srv,
                "PUT",
                &path,
                &[("x-yadb-subject", &subject)],
                &bytes,
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
            let (code, body) =
                http_request(srv, "GET", &path, &[("x-yadb-subject", &subject)], &[])?;
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
            let srv = cli
                .server
                .as_deref()
                .unwrap_or("http://127.0.0.1:8420");
            let data = fs::read(&file)?;
            let path = format!("/s3/{project}/{store}/{bucket}/{}", key.trim_start_matches('/'));
            let (code, body) = http_request(
                srv,
                "PUT",
                &path,
                &[("x-yadb-subject", &subject)],
                &data,
            )?;
            if code != 200 {
                eprintln!("S3 PUT denied/failed ({code}): {}", String::from_utf8_lossy(&body));
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
            let srv = cli
                .server
                .as_deref()
                .unwrap_or("http://127.0.0.1:8420");
            let path = format!("/s3/{project}/{store}/{bucket}/{}", key.trim_start_matches('/'));
            let mut hdrs = vec![("x-yadb-subject", subject.as_str())];
            let range_val;
            if let Some(r) = &range {
                range_val = format!("bytes={r}");
                hdrs.push(("Range", range_val.as_str()));
            }
            let (code, body) = http_request(srv, "GET", &path, &hdrs, &[])?;
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
            let srv = cli
                .server
                .as_deref()
                .unwrap_or("http://127.0.0.1:8420");
            let path = format!("/s3/{project}/{store}/{bucket}?prefix={prefix}");
            let (code, body) =
                http_request(srv, "GET", &path, &[("x-yadb-subject", &subject)], &[])?;
            if code != 200 {
                eprintln!("S3 LS failed ({code}): {}", String::from_utf8_lossy(&body));
                std::process::exit(1);
            }
            let pretty: serde_json::Value = serde_json::from_slice(&body)?;
            println!("{}", serde_json::to_string_pretty(&pretty)?);
        }
        Command::SimCrash { seed } => {
            println!("Running deterministic crash & erasure self-healing simulation (seed={seed})...");
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
