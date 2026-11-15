#!/usr/bin/env python3
"""Start, stop and inspect a persistent local three-node legacy cluster.

`scripts/yadb-local.py` provisions an empty OpenRaft-only deployment: HTTP and
Raft listeners, no client protocols. This launcher is deliberately separate. It
starts the default (legacy) metadata runtime with the PostgreSQL, S3 and iSCSI
listeners enabled, so psql, the AWS CLI and an iSCSI initiator can be used
against a cluster that survives a stop/start cycle.

It is a launcher only. It never wipes a data directory, never changes daemon
defaults and never retries an ambiguous mutation. On the first start it applies
`examples/platform.yaml` once and records that in `state.json`; later starts
reuse the existing data and manifests.
"""
import argparse
from collections import deque
import contextlib
import fcntl
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO = Path(__file__).resolve().parent.parent
BASE = Path('/data/data/com.foxdebug.acodefree/cache/opencode')
DEFAULT_ROOT = BASE / 'yadb-dev'
DEFAULT_PORT_BASE = 18000
NODES = (1, 2, 3)
# One port base derives four contiguous blocks. The offsets leave room for
# every listener and keep deployments with different bases disjoint.
PG_OFFSET = 100
S3_OFFSET = 200
ISCSI_OFFSET = 300
USER = 'backend'
SUBJECT = 'sa:billing-backend'
S3_TARGET = 'billing/receipts'
ISCSI_TARGET = 'iqn.2026-11.io.yadb:billing:vm-root'
DATABASE = 'billing/ledger'
PLATFORM = REPO / 'examples/platform.yaml'
DAEMON = REPO / 'target/debug/yadbd'
START_TIMEOUT = 60
STOP_TIMEOUT = 30


def http_port(base, node):
    return base + node - 1


def pg_port(base, node):
    return base + PG_OFFSET + node - 1


def s3_port(base, node):
    return base + S3_OFFSET + node - 1


def iscsi_port(base, node):
    return base + ISCSI_OFFSET + node - 1


def endpoints(base):
    """Every derived listener as (kind, node, port)."""
    for node in NODES:
        yield 'HTTP', node, http_port(base, node)
        yield 'PostgreSQL', node, pg_port(base, node)
        yield 'S3', node, s3_port(base, node)
        yield 'iSCSI', node, iscsi_port(base, node)


def validate_base(base):
    if type(base) is not int or not 1024 <= base <= 65535 - ISCSI_OFFSET - 2:
        raise RuntimeError(
            f'--port-base {base!r} must be an integer between 1024 and {65535 - ISCSI_OFFSET - 2} '
            f'so that base+{ISCSI_OFFSET}+2 is a valid port')


def resolve_root(path, create):
    path = Path(path).expanduser()
    if path.exists():
        if not path.is_dir():
            raise RuntimeError(f'root {path} exists and is not a directory')
        return path.resolve(strict=True)
    if not create:
        return path.absolute()
    # Resolve only the existing parent so mkdir rejects a symlink atomically.
    try:
        parent = path.parent.resolve(strict=True)
    except OSError as error:
        raise RuntimeError(
            f'parent directory {path.parent} of --root does not exist; create it first') from error
    return parent / path.name


def read_state(root):
    path = root / 'state.json'
    if not path.exists():
        return None
    try:
        state = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f'{path} is unreadable or invalid ({error}); manual diagnosis required') from error
    if not isinstance(state, dict) or not isinstance(state.get('phase'), str):
        raise RuntimeError(f'{path} does not contain a usable phase; manual diagnosis required')
    return state


def write_json(path, obj):
    """Atomically replace a mode-0600 JSON file."""
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w') as handle:
        handle.write(json.dumps(obj, indent=2) + '\n')
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def write_new(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as handle:
        handle.write(data)


@contextlib.contextmanager
def cluster_lock(root):
    """Serialise start/stop without ever guessing about a concurrent caller."""
    handle = (root / 'lock').open('a')
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise RuntimeError(
                f'another start/stop is already operating on {root}; wait for it to finish') from error
        yield handle
    finally:
        handle.close()


def ensure_credentials(root):
    """Create the protocol credentials once; never rotate them on restart."""
    path = root / 'credentials.json'
    if path.exists():
        if path.stat().st_mode & 0o077:
            raise RuntimeError(f'{path} is accessible by group/other; chmod 600 it before starting')
        try:
            users = json.loads(path.read_text())['users']
            secret = users[USER]['secret']
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise RuntimeError(f'{path} is invalid ({error}); manual diagnosis required') from error
        if not isinstance(secret, str) or len(secret) < 12:
            raise RuntimeError(f'{path} has no usable secret for user {USER!r}')
        return secret
    secret = secrets.token_hex(16)
    write_new(path, (json.dumps({'users': {USER: {'subject': SUBJECT, 'secret': secret}}},
                                indent=2) + '\n').encode())
    return secret


def ensure_raft_token(root):
    """Create the shared Raft token once; a changed token breaks the cluster."""
    path = root / 'raft-token'
    if path.exists():
        if path.stat().st_mode & 0o077:
            raise RuntimeError(f'{path} is accessible by group/other; chmod 600 it before starting')
        token = path.read_text().strip()
        if len(token) < 12:
            raise RuntimeError(f'{path} does not contain a usable Raft token')
        return token
    token = secrets.token_hex(32)
    write_new(path, (token + '\n').encode())
    return token


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def pid_cmdline(pid):
    try:
        raw = Path(f'/proc/{pid}/cmdline').read_bytes()
    except OSError:
        return None
    words = [part.decode(errors='replace') for part in raw.split(b'\0') if part]
    # The Raft token is passed in argv, exactly like tests/protocols.py; do not
    # echo the secret back in status output.
    if '--raft-token' in words:
        index = words.index('--raft-token')
        if index + 1 < len(words):
            words[index + 1] = '<redacted>'
    return words


def is_our_daemon(pid, root, node):
    """A recorded PID is only trusted when its argv names this node's data dir."""
    words = pid_cmdline(pid) or []
    return (any(Path(word).name == 'yadbd' for word in words)
            and str(root / 'data' / f'node-{node}') in words)


def port_open(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=1):
            return True
    except OSError:
        return False


def http_get(port, path, timeout=2):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
    except OSError:
        return None, b''


def http_json(port, path, timeout=2):
    status, body = http_get(port, path, timeout)
    try:
        return status, json.loads(body)
    except ValueError:
        return status, None


def http_healthy(port):
    return http_get(port, '/healthz')[0] == 200


def busy_ports(base):
    """Ports already bound on loopback; SO_REUSEADDR ignores TIME_WAIT."""
    busy = []
    for kind, node, port in endpoints(base):
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(('127.0.0.1', port))
        except OSError as error:
            busy.append((kind, node, port, error))
        finally:
            sock.close()
    return busy


def node_command(root, base, node, token):
    """Daemon argv, mirroring tests/protocols.py's legacy cluster.

    `--metadata-runtime` is deliberately absent: legacy is the default. No
    `--admin-subject` is configured, so legacy keeps its historical loopback
    `/v1/apply` behaviour and no subject secret is needed.
    """
    peers = ','.join(f'{n}=127.0.0.1:{http_port(base, n)}' for n in NODES)
    portals = ','.join(f'127.0.0.1:{iscsi_port(base, n)}' for n in NODES)
    return [
        str(DAEMON),
        '--node-id', str(node),
        '--listen', f'127.0.0.1:{http_port(base, node)}',
        '--peers', peers,
        '--data-dir', str(root / 'data' / f'node-{node}'),
        '--protocol-credentials', str(root / 'credentials.json'),
        '--pg-listen', f'127.0.0.1:{pg_port(base, node)}',
        '--iscsi-listen', f'127.0.0.1:{iscsi_port(base, node)}',
        '--iscsi-portals', portals,
        '--s3-listen', f'127.0.0.1:{s3_port(base, node)}',
        '--s3-target', S3_TARGET,
        '--raft-token', token,
    ]


def wait_healthy(base):
    deadline = time.monotonic() + START_TIMEOUT
    pending = set(NODES)
    while pending and time.monotonic() < deadline:
        pending = {node for node in pending if not http_healthy(http_port(base, node))}
        if pending:
            time.sleep(0.1)
    if pending:
        raise RuntimeError(f'HTTP health check did not answer on node(s) {sorted(pending)} within {START_TIMEOUT}s')


def wait_leader(base):
    """Read-only wait for one agreed legacy Raft leader before any mutation."""
    deadline = time.monotonic() + START_TIMEOUT
    while time.monotonic() < deadline:
        statuses = [http_json(http_port(base, node), '/raft/status')[1] for node in NODES]
        if all(isinstance(status, dict) and status.get('leader_id') is not None for status in statuses):
            leaders = {status['leader_id'] for status in statuses}
            if len(leaders) == 1:
                return leaders.pop()
        time.sleep(0.1)
    raise RuntimeError(f'no agreed Raft leader on all three nodes within {START_TIMEOUT}s')


def wait_control(base):
    deadline = time.monotonic() + START_TIMEOUT
    pending = set(NODES)
    while pending and time.monotonic() < deadline:
        for node in list(pending):
            status, body = http_json(http_port(base, node), '/v1/status')
            if status == 200 and isinstance(body, dict) and DATABASE in body.get('databases', {}):
                pending.discard(node)
        if pending:
            time.sleep(0.1)
    if pending:
        raise RuntimeError(
            f'control-plane cache did not report {DATABASE} on node(s) {sorted(pending)} within '
            f'{START_TIMEOUT}s; the manifests may not have been applied')


def apply_platform(base):
    body = json.dumps({'yaml': PLATFORM.read_text()}).encode()
    request = urllib.request.Request(
        f'http://127.0.0.1:{http_port(base, 1)}/v1/apply', data=body,
        headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status, data = response.status, response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f'POST /v1/apply failed with HTTP {error.code}: {error.read().decode(errors="replace")}') from error
    except OSError as error:
        raise RuntimeError(f'POST /v1/apply failed: {error}') from error
    if not 200 <= status < 300:
        raise RuntimeError(f'POST /v1/apply failed with HTTP {status}: {data.decode(errors="replace")}')


def terminate(children):
    """SIGTERM the recorded children and report each exit without killing."""
    for child in children:
        if child.poll() is None:
            try:
                child.send_signal(signal.SIGTERM)
            except OSError as error:
                print(f'PID {child.pid}: SIGTERM failed: {error}', file=sys.stderr)
    deadline = time.monotonic() + STOP_TIMEOUT
    outcomes = []
    for child in children:
        try:
            code = child.wait(timeout=max(0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            code = None
        outcomes.append((child.pid, code))
    return outcomes


def print_endpoints(base):
    print(f'  HTTP        {", ".join(f"node {n} http://127.0.0.1:{http_port(base, n)}" for n in NODES)}')
    print(f'  PostgreSQL  {", ".join(f"node {n} 127.0.0.1:{pg_port(base, n)}" for n in NODES)}')
    print(f'  S3          {", ".join(f"node {n} http://127.0.0.1:{s3_port(base, n)}" for n in NODES)}')
    print(f'  iSCSI       {", ".join(f"node {n} 127.0.0.1:{iscsi_port(base, n)}" for n in NODES)}')
    print(f'  S3 target   {S3_TARGET}')
    print(f'  iSCSI target {ISCSI_TARGET}')


def print_credentials(root, secret):
    print(f'credentials: {root / "credentials.json"} (mode 0600)')
    print(f'  PostgreSQL user / S3 access key / iSCSI CHAP name: {USER}')
    print(f'  password: {secret}')
    print(f'  PostgreSQL database: {DATABASE}')


def cmd_start(args):
    os.umask(0o077)
    validate_base(args.port_base)
    root = resolve_root(args.root, create=True)
    if not DAEMON.is_file():
        raise RuntimeError(
            f'missing daemon binary {DAEMON}; build it first with:\n'
            f'  flock /data/data/com.foxdebug.acodefree/cache/opencode/cargo.lock env '
            f'CARGO_INCREMENTAL=0 cargo build -p yadbd --offline --locked -j 2')
    try:
        root.mkdir(mode=0o700, exist_ok=True)
    except OSError as error:
        raise RuntimeError(
            f'cannot create root {root}: {error}; the parent directory must already exist') from error
    with cluster_lock(root):
        state = read_state(root)
        data_dirs = [root / 'data' / f'node-{node}' for node in NODES]
        if state is None and any(path.exists() for path in data_dirs):
            raise RuntimeError(
                f'{root} contains node data but no state.json; refusing to guess how it was created. '
                f'Inspect {root} and remove it only if you are certain it is disposable')
        if state is not None:
            recorded_base = state.get('port_base')
            if recorded_base != args.port_base:
                raise RuntimeError(
                    f'state.json records --port-base {recorded_base} for this root; the data dirs and '
                    f'peer map are bound to it. Rerun with --port-base {recorded_base}, or use a new --root')
        phase = state['phase'] if state is not None else None
        pids = state.get('pids', []) if state is not None else []
        if phase == 'running':
            if len(pids) == 3 and all(pid_alive(pid) and is_our_daemon(pid, root, node)
                                      for node, pid in zip(NODES, pids)):
                raise RuntimeError(
                    f'cluster is already running (pids {pids}); use '
                    f'`{sys.argv[0]} stop --root {root}` first')
            if not args.force:
                raise RuntimeError(
                    f'state.json says phase "running" but the recorded processes are gone or foreign '
                    f'(pids {pids}). No data was changed. Inspect with '
                    f'`{sys.argv[0]} status --root {root}`, then rerun start with --force if the '
                    f'processes really are gone. Data dirs and credentials are reused; manifests are not re-applied')
            print('WARNING: stale "running" state; continuing because --force was given.', file=sys.stderr)
        elif phase == 'stopped':
            alive = [pid for pid in pids if pid_alive(pid)]
            if alive:
                if not args.force:
                    raise RuntimeError(
                        f'state.json says phase "stopped" but recorded PID(s) {alive} are alive. '
                        f'Inspect with `{sys.argv[0]} status --root {root}`; use stop to terminate them, '
                        f'or rerun start with --force only after confirming they are this cluster')
                print(f'WARNING: "stopped" state has live recorded PID(s) {alive}; continuing with --force.',
                      file=sys.stderr)
        elif phase is not None:
            if not args.force:
                raise RuntimeError(
                    f'state.json phase {phase!r} is not a settled state. Inspect '
                    f'`{sys.argv[0]} status --root {root}` and the logs, then rerun start with --force '
                    f'if the recorded processes are gone. No data is wiped or re-initialised')
            print(f'WARNING: previous phase {phase!r}; continuing because --force was given.', file=sys.stderr)

        busy = busy_ports(args.port_base)
        if busy:
            details = '\n'.join(f'  {kind} node {node} port {port}: {error}' for kind, node, port, error in busy)
            raise RuntimeError(f'refusing to start; derived ports are already in use:\n{details}')

        secret = ensure_credentials(root)
        token = ensure_raft_token(root)
        applied = bool(state and state.get('applied'))
        apply_attempted = bool(state and state.get('apply_attempted'))
        # An apply that was never sent is not a retry; an apply whose outcome is
        # unknown is never sent again.
        should_apply = not applied and not apply_attempted
        if apply_attempted and not applied:
            print('WARNING: a previous /v1/apply was attempted but never confirmed. It is not '
                  'retried automatically; inspect the control plane with status/logs.', file=sys.stderr)

        record = {
            'version': 1,
            'phase': 'starting',
            'port_base': args.port_base,
            'pids': [],
            'apply_attempted': apply_attempted,
            'applied': applied,
            'started_at': None,
        }
        write_json(root / 'state.json', record)
        children = []
        try:
            for node in NODES:
                log_path = root / 'logs' / f'node-{node}.log'
                log_path.parent.mkdir(mode=0o700, exist_ok=True)
                log = log_path.open('ab', buffering=0)
                try:
                    child = subprocess.Popen(
                        node_command(root, args.port_base, node, token),
                        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                finally:
                    log.close()
                children.append(child)
                record['pids'] = [process.pid for process in children]
                write_json(root / 'state.json', record)
            print(f'started pids {record["pids"]}; waiting for readiness', flush=True)
            wait_healthy(args.port_base)
            leader = wait_leader(args.port_base)
            print(f'one Raft leader observed on every node (node {leader})')
            if should_apply:
                record['apply_attempted'] = True
                write_json(root / 'state.json', record)
                print(f'applying {PLATFORM} once (first start)', flush=True)
                apply_platform(args.port_base)
                record['applied'] = True
                write_json(root / 'state.json', record)
            if record['applied']:
                wait_control(args.port_base)
            record['phase'] = 'running'
            record['started_at'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            write_json(root / 'state.json', record)
        except BaseException as error:
            outcomes = terminate(children)
            alive = [pid for pid, code in outcomes if pid_alive(pid)]
            record.update(phase='stopping' if alive else 'failed', pids=alive,
                          exit_codes=outcomes, error=f'{type(error).__name__}: {error}')
            write_json(root / 'state.json', record)
            raise
    print('\nCluster running (legacy metadata runtime)')
    print(f'root: {root}')
    print(f'port base: {args.port_base}')
    print_endpoints(args.port_base)
    print_credentials(root, secret)
    if should_apply:
        print(f'control plane: applied {PLATFORM.name} on first start')
    else:
        print('control plane: existing data and manifests reused; examples/platform.yaml was not re-applied')


def cmd_stop(args):
    os.umask(0o077)
    root = resolve_root(args.root, create=False)
    if not (root / 'state.json').exists():
        raise RuntimeError(f'{root} has no state.json; nothing recorded to stop')
    with cluster_lock(root):
        state = read_state(root)
        base = state.get('port_base')
        validate_base(base)
        pids = state.get('pids', [])
        if pids and len(pids) != 3:
            raise RuntimeError(f'state.json records {len(pids)} PIDs, expected 3; manual diagnosis required')
        if not pids:
            if state['phase'] == 'stopped':
                print('cluster already stopped (phase "stopped")')
            else:
                print(f'no PIDs recorded (phase {state["phase"]!r}); marking the cluster stopped')
            busy = busy_ports(base)
            if busy:
                print('INCONSISTENT: a derived port is still bound:')
                for kind, node, port, error in busy:
                    print(f'  {kind} node {node} port {port}: {error}')
                return 1
            if state['phase'] != 'stopped':
                state.update(phase='stopped', pids=[],
                             stopped_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
                write_json(root / 'state.json', state)
            return 0

        live = []
        for node, pid in zip(NODES, pids):
            if not pid_alive(pid):
                print(f'node {node} PID {pid}: already absent')
                continue
            if not is_our_daemon(pid, root, node):
                raise RuntimeError(
                    f'refusing to signal PID {pid}: its command line is not this root\'s node-{node} '
                    f'daemon. Manual diagnosis required; nothing was signalled')
            live.append((node, pid))
        for node, pid in live:
            try:
                os.kill(pid, signal.SIGTERM)
                print(f'node {node} PID {pid}: SIGTERM sent')
            except OSError as error:
                print(f'node {node} PID {pid}: SIGTERM failed: {error}', file=sys.stderr)

        deadline = time.monotonic() + STOP_TIMEOUT
        still = []
        for node, pid in live:
            while pid_alive(pid) and time.monotonic() < deadline:
                time.sleep(0.1)
            if pid_alive(pid):
                print(f'node {node} PID {pid}: STILL RUNNING after {STOP_TIMEOUT}s; not killing it')
                still.append((node, pid))
            else:
                print(f'node {node} PID {pid}: exited')

        if still:
            state.update(phase='stopping', pids=[pid for _, pid in still],
                         error=f'PID(s) did not exit after SIGTERM: {still}')
            write_json(root / 'state.json', state)
            raise RuntimeError(
                f'PID(s) {still} did not exit after SIGTERM; not killing them. Inspect and stop manually')
        busy = busy_ports(base)
        state.update(phase='stopped' if not busy else 'incomplete', pids=[],
                     stopped_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                     exit_codes=[{'node': node, 'pid': pid} for node, pid in zip(NODES, pids)])
        if busy:
            state['error'] = 'derived port(s) still bound after all PIDs exited'
        write_json(root / 'state.json', state)
        if busy:
            print('INCONSISTENT: all daemons exited but a derived port is still bound:')
            for kind, node, port, error in busy:
                print(f'  {kind} node {node} port {port}: {error}')
            return 1
        print(f'cluster stopped; data remains in {root / "data"}')
        return 0


def cmd_status(args):
    root = resolve_root(args.root, create=False)
    state = read_state(root) if (root / 'state.json').exists() else None
    print(f'root: {root}')
    if state is None:
        print(f'phase: not started (no state.json)')
        print(f'port base would be: {args.port_base}')
        print_endpoints(args.port_base)
        busy = busy_ports(args.port_base)
        if busy:
            print('note: some derived ports are currently in use by another process:')
            for kind, node, port, error in busy:
                print(f'  {kind} node {node} port {port}: {error}')
        return 0

    base = state.get('port_base')
    try:
        validate_base(base)
    except RuntimeError as error:
        print(f'INCONSISTENT: {error}')
        return 1
    phase = state['phase']
    pids = state.get('pids', [])
    problems = []
    print(f'phase: {phase}')
    print(f'port base: {base}')

    alive = {}
    if pids:
        if len(pids) != 3:
            problems.append(f'{phase} state records {len(pids)} PIDs, expected 3')
        for node, pid in zip(NODES, pids):
            present = pid_alive(pid)
            alive[node] = present
            words = pid_cmdline(pid)
            print(f'pid {pid} (node {node}): {"alive" if present else "absent"}; cmdline={words}')
            if present and not is_our_daemon(pid, root, node):
                problems.append(f'PID {pid} is alive but is not this root\'s node-{node} daemon')

    http = {node: http_healthy(http_port(base, node)) for node in NODES}
    pg = {node: port_open(pg_port(base, node)) for node in NODES}
    s3 = {node: port_open(s3_port(base, node)) for node in NODES}
    iscsi = {node: port_open(iscsi_port(base, node)) for node in NODES}
    print('HTTP:      ', {f'node {node}': 'ok' if ok else 'down' for node, ok in http.items()})
    print('PostgreSQL:', {f'node {node}': 'open' if ok else 'closed' for node, ok in pg.items()})
    print('S3:        ', {f'node {node}': 'open' if ok else 'closed' for node, ok in s3.items()})
    print('iSCSI:     ', {f'node {node}': 'open' if ok else 'closed' for node, ok in iscsi.items()})
    print_credentials_path(root)

    if phase == 'running':
        if len(pids) != 3:
            problems.append('running state does not record three PIDs')
        for node, pid in zip(NODES, pids):
            if not alive.get(node):
                problems.append(f'recorded PID {pid} for node {node} is absent')
        for node in NODES:
            if not http[node]:
                problems.append(f'HTTP port {http_port(base, node)} is not healthy')
            if not pg[node]:
                problems.append(f'PostgreSQL port {pg_port(base, node)} is not accepting connections')
            if not s3[node]:
                problems.append(f'S3 port {s3_port(base, node)} is not accepting connections')
            if not iscsi[node]:
                problems.append(f'iSCSI port {iscsi_port(base, node)} is not accepting connections')
        if state.get('applied'):
            for node in NODES:
                status, body = http_json(http_port(base, node), '/v1/status')
                if status != 200 or not isinstance(body, dict) or DATABASE not in body.get('databases', {}):
                    problems.append(f'node {node} does not report {DATABASE} in its control-plane cache')
    elif phase == 'stopped':
        for node, pid in zip(NODES, pids):
            if alive.get(node):
                problems.append(f'stopped state still has live recorded PID {pid}')
        busy = busy_ports(base)
        for kind, node, port, error in busy:
            problems.append(f'stopped state still has a listener on {kind} port {port} ({error})')
    else:
        problems.append(f'phase {phase!r} is not a settled state; run start --force or stop')

    if problems:
        print('INCONSISTENT:')
        for problem in problems:
            print(f'  - {problem}')
        return 1
    print('consistent')
    return 0


def print_credentials_path(root):
    path = root / 'credentials.json'
    if path.exists():
        print(f'credentials: {path} (user {USER}, database {DATABASE})')


def cmd_logs(args):
    root = resolve_root(args.root, create=False)
    found = False
    for node in NODES:
        path = root / 'logs' / f'node-{node}.log'
        print(f'==> node {node}: {path} <==')
        if not path.exists():
            print('(no log file)')
            continue
        found = True
        with path.open('r', errors='replace') as handle:
            for line in deque(handle, maxlen=args.lines):
                print(line, end='' if line.endswith('\n') else '\n')
    return 0 if found else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)

    start = sub.add_parser('start', help='start the cluster (first start also applies examples/platform.yaml)')
    start.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    start.add_argument('--port-base', type=int, default=DEFAULT_PORT_BASE)
    start.add_argument('--force', action='store_true',
                       help='proceed past a stale or failed state; never wipes data or re-applies manifests')

    stop = sub.add_parser('stop', help='SIGTERM the three recorded daemons and wait')
    stop.add_argument('--root', type=Path, default=DEFAULT_ROOT)

    status = sub.add_parser('status', help='compare state.json with live PIDs and derived ports')
    status.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    status.add_argument('--port-base', type=int, default=DEFAULT_PORT_BASE,
                        help='only used to describe ports when no state.json exists')

    logs = sub.add_parser('logs', help='tail each node log')
    logs.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    logs.add_argument('--lines', type=int, default=40)

    args = parser.parse_args()
    if getattr(args, 'lines', 1) < 1:
        parser.error('--lines must be at least 1')
    if args.command == 'start':
        cmd_start(args)
    elif args.command == 'stop':
        return cmd_stop(args)
    elif args.command == 'status':
        return cmd_status(args)
    else:
        return cmd_logs(args)


if __name__ == '__main__':
    try:
        sys.exit(main() or 0)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f'Error: {error}', file=sys.stderr)
        sys.exit(1)
