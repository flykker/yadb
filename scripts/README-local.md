# Experimental empty local cluster

This creates a **new, empty, nonproduction** cluster only. It does not migrate or
modify an existing cluster. The experimental runtime has unresolved replication,
authorization freshness and lifecycle failures; `production_ready` remains false.
Starting processes or inspecting status does not prove replication or readiness.

Requirements: Python 3 with `cryptography`, Rust/Cargo with dependencies already
cached, and unused loopback ports. One deployment owns HTTP ports `base..base+2`
and Raft ports `base+10000..base+10008`; the default base is 8420, i.e. HTTP
8420–8422 and Raft 18420–18428. Preparation builds `yadbd` offline/locked with two
jobs, then copies the binary into a fresh private directory under
`/data/data/com.foxdebug.acodefree/cache/opencode`. It prints that directory even
if a later provisioning step fails. It does not start the cluster.

```sh
python3 scripts/yadb-local.py prepare
# Or choose a new, not-yet-existing directory under an existing parent:
python3 scripts/yadb-local.py prepare --root /public/cluster_data
# To run a second deployment in parallel, move every listener off the default base:
python3 scripts/yadb-local.py prepare --root /public/cluster_data_b --port-base 9420
# After reviewing the printed root and configs:
python3 scripts/yadb-local.py run /absolute/path/to/yadb-local-...
# In another terminal, check state.json against live PIDs, command lines and ports:
python3 scripts/yadb-local.py status /absolute/path/to/yadb-local-...
# Then request local status with signed subject headers:
python3 scripts/yadb-local.py verify /absolute/path/to/yadb-local-...
```

The chosen base is recorded in `config/ports.json`; `run`, `status` and `verify`
read it back, so `--port-base` is only needed for `prepare`. The recorded base is
fixed for the life of a deployment: changing ports requires a new root, never an
edit of `ports.json`. A base is rejected
unless it leaves room for all twelve listeners below 65536. Deployments prepared
with neither `ports.json` nor `identity.json` fall back to 8420 with a warning
that identity cannot be verified. Two deployments may coexist only if
their port ranges are disjoint; nothing coordinates that beyond the pre-launch
bind check.

Preparation also writes mode-0600 `config/identity.json`, containing a random
deployment ID, node IDs, port base, and SHA-256 hash of the canonical HTTP and
Raft listener configuration (including Raft peer addresses). Before `run`,
`status`, `verify`, or either smoke command makes requests, the scripts check
both files exist, the bases agree and are in range, and the listener hash matches
the group configs. A mismatch, missing counterpart, or unreadable configuration
fails closed. This checks local file integrity, not the identity of a listening
server or authentication; it cannot stop a caller deliberately editing the files
and recomputing the hash. Protect the deployment directory.

`prepare` builds with `--features experimental-openraft-storage` and copies
`target/debug/yadbd` immediately, so it always installs a feature-enabled daemon.
Do not copy that path after an unrelated default-feature build: `cargo test
--workspace` and `cargo build` without the feature overwrite `target/debug/yadbd`
with a daemon that cannot start OpenRaft. Re-run the feature build before any
manual install, and treat a missing internal-snapshot endpoint or a
`metadata_runtime` other than `openraft` as the wrong binary.

`run` starts every daemon with `--admin-subject local-operator` and exports
`YADB_SUBJECT_SECRET` from `credentials/secrets.json`. `/v1/apply` therefore
accepts only a request carrying `x-yadb-subject: local-operator` together with
the matching HMAC-SHA256 `x-yadb-subject-signature`; unsigned, forged, or
non-allowlisted subjects are refused (401/403). The smoke script signs its apply
request with the same secret. A daemon started with an allowlist but no secret,
or in OpenRaft mode with an empty allowlist, refuses to start. The operator
identity lives in one constant (`OPERATOR`) used by the launch argv, the startup
readiness probe and `verify`, so those three cannot drift apart.

`run` holds the terminal until interrupted and is the supervisor; it is not a
background daemonizer. Start it under a supervisor or in a background shell, then
use `status` from another shell. Stopping it with SIGTERM/SIGINT stops all three
children and records `phase: stopped` with per-child exit codes; a nonzero exit
code or an unclean stop records `incomplete` and preserves artifacts without an
automatic restart.

`status` is read-only. It fails with a nonzero exit when `state.json` says
`running` but a recorded process is absent or foreign, an HTTP endpoint does not
answer its health check or a Raft port does not accept connections, or a
`stopped`/`prepared` state still has live processes, an HTTP endpoint answering
health checks, or an open Raft port. A foreign listener that does not answer the
health check is not detected as an HTTP listener. It does not prove replication,
authorization freshness, or data integrity.

Run stays in the foreground; Ctrl-C requests graceful shutdown of all three
children. Only a clean shutdown permits a later explicit `run`, which changes
all nine group configs to open mode without initialization. An interrupted or
failed startup/shutdown is marked incomplete and requires manual diagnosis; no
automatic retry, deletion, fallback, or recovery is attempted. Logs are in
`logs/`, process IDs and exit codes in `state.json`, and private credentials in
`credentials/`. Preserve partial artifacts. Ports are checked before launch but
another process can race the check; existing listeners are never killed.

The cluster has three replicas in each of control group 1 and data groups 2 and
3, with project IDs 1→2 and 2→3; no projects or sample data are created. Raft
uses fresh per-node Ed25519 signing keys and a fresh CA with unique pinned node
certificates, for mutual TLS. HTTP and Raft bind only 127.0.0.1; no PG, S3-wire,
or iSCSI listener is enabled. The status requests carry signed subject headers,
but the daemon's health and status endpoints ignore those headers; these requests
are not authenticated verification. This does not establish that authorization
freshness is safe. Protect the
entire deployment directory and note that local process environments can expose
secrets to sufficiently privileged users. Certificates expire after 30 days.

The generated local configuration uses an opt-in **soft** leadership preference:
group 1 prefers node 1, group 2 prefers node 2, and group 3 prefers node 3.
Each group uses `heartbeat_ms: 250`, `election_min_ms: 1500`,
`nonpreferred_election_min_ms: 2750`, and a common `election_max_ms: 3000`.
All replicas in a group must use matching timing and preference values. This is
only a probabilistic election bias: a nonpreferred node may legitimately become
leader, and a returning preferred node does not automatically reclaim leadership.
Automatic elections and failover remain enabled for every voter. Replacing the
binary does not change an existing deployment; update all group configs
consistently and perform an explicit clean stop/reopen. A one-leader-per-node
observation balances group leadership counts only; it does not guarantee equal
SQL, S3, block, CPU, or storage workload.

See `docs/openraft-empty-test-cluster.md` for the known limitations. Do not
interpret a successful status request as a write/replication test or resolve an
ambiguous mutation by blindly retrying it.

## Local smoke readback

On a running **empty** local cluster, explicitly run once:

```sh
python3 scripts/yadb-local-smoke.py setup /absolute/path/to/yadb-local-...
python3 scripts/yadb-local-smoke.py check /absolute/path/to/yadb-local-...
# After a clean stop and explicit reopen, check again:
python3 scripts/yadb-local-smoke.py check /absolute/path/to/yadb-local-...
```

Setup applies sample resources and writes SQL, object and block data through
node 1. It creates a permanent `logs/smoke-setup-started` marker before the first
mutation and refuses repeat setup. A failure or timeout can leave an ambiguous
partial setup: preserve the marker and evidence and investigate; never blindly
retry. Check performs only read-only requests (including SQL SELECT via POST),
polls with a nominal 30-second window per check, plus time for in-flight requests
and polling overhead; this is not a strict wall-clock deadline. It compares
responses through all three HTTP endpoints. Both commands perform the identity
preflight before creating evidence or the setup marker; the legacy fallback above
still permits unverified operation with a warning. Never point a smoke run at a
root you did not prepare. Private uniquely named
JSON evidence in `logs/` includes HTTP outcomes, leader headers and group statuses.
Successful readback through all three HTTP endpoints, including after reopening,
does not prove independent replica-local application, durable replication, or
production readiness.
