use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use yadb_blob::{BlobId, ErasureScheme, PoolStriper, VolumeConfig, VolumeStore};
use yadb_control::{parse_manifests, ControlPlaneState, DiffOp, Role};
use yadb_io::{FaultConfig, OsFs, SimFs, StorageFs};

#[derive(Parser)]
#[command(
    name = "yadbctl",
    version,
    about = "Declarative control & storage CLI for YADB (Yet Another DB & Data Platform)"
)]
struct Cli {
    /// Path to local state file (simulating MetaKV persistence until Raft node daemon is started)
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
    /// Apply declarative YAML manifests and advance cluster generation
    Apply {
        #[arg(short = 'f', long)]
        file: PathBuf,
    },
    /// Show current cluster generation, pools, projects, databases, and object stores
    Status,
    /// Verify local zero-hop RBAC authorization for a subject on a project
    AuthCheck {
        #[arg(long)]
        project: String,
        #[arg(long)]
        subject: String,
        #[arg(long, default_value_t = false)]
        write: bool,
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.cmd {
        Command::Plan { file } => {
            let yaml = fs::read_to_string(&file)?;
            let manifests = parse_manifests(&yaml)?;
            let state = load_state(&cli.state)?;
            let (ops, next) = state.plan(&manifests)?;
            println!(
                "Plan OK (current generation: {}, next generation: {}):",
                state.generation, next.generation
            );
            print_ops(&ops);
        }
        Command::Apply { file } => {
            let yaml = fs::read_to_string(&file)?;
            let manifests = parse_manifests(&yaml)?;
            let mut state = load_state(&cli.state)?;
            let ops = state.apply(&manifests)?;
            save_state(&cli.state, &state)?;
            println!("Applied successfully -> generation {}", state.generation);
            print_ops(&ops);
        }
        Command::Status => {
            let state = load_state(&cli.state)?;
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
        }
        Command::AuthCheck {
            project,
            subject,
            write,
        } => {
            let state = load_state(&cli.state)?;
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

            // Inject torn write + power cut + bitrot on domain-0 and complete loss of domain-2!
            fs.set_faults(FaultConfig {
                seed: seed ^ 0xDEAD_BEEF,
                torn_write_prob: 1.0,
                read_eio_prob: 0.0,
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

            // Reopen all domains after crash and verify + repair every single blob!
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
            let block = vec![0xABu8; 64 * 1024]; // 64 KiB SST/chunk
            let count = 64u32; // 4 MiB logical payload
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
