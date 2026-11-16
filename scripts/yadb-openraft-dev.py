#!/usr/bin/env python3
"""Start, stop and inspect a disposable three-node Multi-Raft OpenRaft cluster.

This launcher is the explicit-OpenRaft sibling of `scripts/yadb-dev.py` (which
keeps legacy as the default). It provisions a *fresh* root containing three
daemons, each running three mandatory-mTLS Raft groups: one control group plus
two data groups. The project route map is `{"1": 2, "2": 3}`, so project IDs 1
and 2 are served by different data groups.

Configuration, manifests, TLS/signing material and the on-disk layout reuse the
exact version-1 group config / version-2 manifest format and certificate
generation already used by `scripts/yadb-local.py`; this script imports those
helpers instead of inventing a second format. It also enables the PostgreSQL,
S3 and iSCSI listeners so the same protocols as the legacy launcher can be
exercised against the OpenRaft runtime.

The daemon is started with an explicit `--metadata-runtime openraft`, the
feature-enabled binary, a signed admin allowlist (`local-operator`) and a
non-empty shared secret. A control-plane apply therefore has to be signed; use
the `apply` subcommand rather than an unauthenticated curl.

It is a launcher only: it never wipes a data directory, never changes daemon
defaults, never retries an ambiguous mutation and never switches an existing
legacy root. On the first start it applies `examples/platform.yaml` once and
records that in `state.json`.

`restart-node`, `stop-node` and `start-node` exist for failure drills: they keep
`state.json` truthful while a node is intentionally absent (a `null` PID and a
`down` list), so `status` does not report an intentional outage as corruption.
"""
import argparse
from collections import deque
import contextlib
import fcntl
import hashlib
import hmac
import importlib.util
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
import uuid

REPO = Path(__file__).resolve().parent.parent
BASE = Path('/data/data/com.foxdebug.acodefree/cache/opencode')
DEFAULT_ROOT = BASE / 'yadb-openraft-dev'
DEFAULT_PORT_BASE = 23500
NODES = (1, 2, 3)
GROUPS = (1, 2, 3)
CONTROL_GROUP = 1
PROJECT_GROUPS = {'1': 2, '2': 3}

# One base derives five disjoint blocks. The offsets keep the deployment in
# 23500..239xx for the default base and leave every listener well clear of the
# legacy launcher's 18000+ range.
HTTP_OFFSET = 0
RAFT_OFFSET = 100
PG_OFFSET = 200
S3_OFFSET = 300
ISCSI_OFFSET = 400
MAX_OFFSET = ISCSI_OFFSET + 2

# Protocol identity. `sa:billing-backend` is a readWrite member of the
# `billing` project that examples/platform.yaml creates; the PostgreSQL user,
# S3 access key and iSCSI CHAP name all resolve to that subject.
USER = 'backend'
MEMBER_SUBJECT = 'sa:billing-backend'
# The only subject on every daemon's --admin-subject allowlist. Signed applies
# use it, so the launcher and the evidence commands cannot drift apart.
ADMIN = 'local-operator'
S3_TARGET = 'billing/receipts'
ISCSI_TARGET = 'iqn.2026-11.io.yadb:billing:vm-root'
PLATFORM = REPO / 'examples/platform.yaml'
DAEMON = REPO / 'target/debug/yadbd'
START_TIMEOUT = 90
STOP_TIMEOUT = 30
BUILD_HINT = (
    'flock /data/data/com.foxdebug.acodefree/cache/opencode/cargo.lock env '
    'CARGO_INCREMENTAL=0 cargo build -p yadbd --features openraft-storage '
    '--offline --locked -j 2')


def _local_helpers():
    """Load the shared provisioning helpers from scripts/yadb-local.py.

    The file name contains a hyphen, so it cannot be imported normally. The
    module only defines constants and functions; importing it has no side
    effects. Reusing it keeps certificate generation and JSON writing byte-for-
    byte identical to the existing OpenRaft launcher.
    """
    path = REPO / 'scripts' / 'yadb-local.py'
    spec = importlib.util.spec_from_file_location('yadb_local_shared', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


LOCAL = _local_helpers()


def http_port(base, node):
    return base + HTTP_OFFSET + node - 1


def raft_port(base, group, node):
    return base + RAFT_OFFSET + (group - 1) * 3 + node - 1


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
    for group in GROUPS:
        for node in NODES:
            yield f'Raft group {group}', node, raft_port(base, group, node)


def validate_base(base):
    if type(base) is not int or not 1024 <= base <= 65535 - MAX_OFFSET:
        raise RuntimeError(
            f'--port-base {base!r} must be an integer between 1024 and '
            f'{65535 - MAX_OFFSET} so that base+{MAX_OFFSET} is a valid port')


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


def pid_alive(pid):
    return bool(pid) and LOCAL.pid_alive(pid)


def pid_cmdline(pid):
    words = LOCAL.pid_cmdline(pid) or []
    # Secrets are passed through the environment, not argv, but redact the
    # token anyway if an older invocation put it on the command line.
    if '--raft-token' in words:
        index = words.index('--raft-token')
        if index + 1 < len(words):
            words[index + 1] = '<redacted>'
    if '--subject-secret' in words:
        index = words.index('--subject-secret')
        if index + 1 < len(words):
            words[index + 1] = '<redacted>'
    return words


def is_our_daemon(pid, root, node):
    """A recorded PID is only trusted when its argv names this node's data dir."""
    words = pid_cmdline(pid)
    return (any(Path(word).name == 'yadbd' for word in words)
            and str(root / 'data' / f'node-{node}') in words)


def port_open(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=1):
            return True
    except OSError:
        return False


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


def subject_signature(secret, subject):
    return hmac.new(secret.encode(), subject.encode(), hashlib.sha256).hexdigest()


def signed_headers(secret, subject):
    return {'x-yadb-subject': subject,
            'x-yadb-subject-signature': subject_signature(secret, subject)}


def http_get(port, path, headers=None, timeout=2):
    request = urllib.request.Request(f'http://127.0.0.1:{port}{path}', headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
    except OSError:
        return None, b''


def http_json(port, path, headers=None, timeout=2):
    status, body = http_get(port, path, headers, timeout)
    try:
        return status, json.loads(body)
    except ValueError:
        return status, None


def http_healthy(port):
    return http_get(port, '/healthz')[0] == 200


def load_secrets(root):
    path = root / 'credentials' / 'secrets.json'
    try:
        secrets = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f'{path} is unreadable or invalid ({error}); manual diagnosis required') from error
    for field in ('subject', 'raft'):
        if not isinstance(secrets.get(field), str) or len(secrets[field]) < 12:
            raise RuntimeError(f'{path} has no usable {field!r} secret')
    return secrets


def ensure_credentials(root):
    """Create the protocol user once; never rotate it on restart."""
    path = root / 'credentials.json'
    if path.exists():
        if path.stat().st_mode & 0o077:
            raise RuntimeError(f'{path} is accessible by group/other; chmod 600 it before starting')
        try:
            secret = json.loads(path.read_text())['users'][USER]['secret']
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise RuntimeError(f'{path} is invalid ({error}); manual diagnosis required') from error
        if not isinstance(secret, str) or len(secret) < 12:
            raise RuntimeError(f'{path} has no usable secret for user {USER!r}')
        return secret
    secret = secrets.token_hex(16)
    LOCAL.save(path, {'users': {USER: {'subject': MEMBER_SUBJECT, 'secret': secret}}})
    return secret


def listener_hash(root, base):
    """Hash HTTP listeners and Raft listeners/peer addresses from group configs."""
    config = root / 'config'
    listeners = {'http': [f'127.0.0.1:{http_port(base, node)}' for node in NODES],
                 'raft': []}
    for node in NODES:
        for group in GROUPS:
            cfg = LOCAL.load(config / f'node-{node}-group-{group}.json')
            listeners['raft'].append({
                'node': cfg['node'], 'group': cfg['group'], 'listen': cfg['listen'],
                'peers': [{'id': peer['id'], 'address': peer['address']}
                          for peer in cfg['peers']]})
    canonical = json.dumps(listeners, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(canonical).hexdigest()


def node_command(root, base, node):
    """Daemon argv for one node; secrets arrive through the environment."""
    peers = ','.join(f'{n}=127.0.0.1:{http_port(base, n)}' for n in NODES)
    portals = ','.join(f'127.0.0.1:{iscsi_port(base, n)}' for n in NODES)
    return [
        str(DAEMON),
        '--listen', f'127.0.0.1:{http_port(base, node)}',
        '--node-id', str(node),
        '--data-dir', str(root / 'data' / f'node-{node}'),
        '--metadata-runtime', 'openraft',
        '--openraft-config', str(root / 'config' / f'node-{node}-manifest.json'),
        '--peers', peers,
        '--admin-subject', ADMIN,
        '--protocol-credentials', str(root / 'credentials.json'),
        '--pg-listen', f'127.0.0.1:{pg_port(base, node)}',
        '--s3-listen', f'127.0.0.1:{s3_port(base, node)}',
        '--s3-target', S3_TARGET,
        '--iscsi-listen', f'127.0.0.1:{iscsi_port(base, node)}',
        '--iscsi-portals', portals,
    ]


def daemon_env(root):
    secrets_ = load_secrets(root)
    env = os.environ.copy()
    env.update(YADB_SUBJECT_SECRET=secrets_['subject'], YADB_RAFT_TOKEN=secrets_['raft'])
    return env


def provision(root, base):
    """Create credentials, group configs and manifests in a fresh root."""
    os.umask(0o077)
    validate_base(base)
    for directory in ('credentials', 'config', 'metadata', 'data', 'logs'):
        (root / directory).mkdir(mode=0o700)
    credential_dir = root / 'credentials'
    ca_key = LOCAL.ed25519.Ed25519PrivateKey.generate()
    now = LOCAL.datetime.datetime.utcnow()
    ca_name = LOCAL.name('yadb openraft dev ' + str(uuid.uuid4()))
    ca = (LOCAL.x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(LOCAL.x509.random_serial_number())
          .not_valid_before(now - LOCAL.datetime.timedelta(minutes=5))
          .not_valid_after(now + LOCAL.datetime.timedelta(days=30))
          .add_extension(LOCAL.x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(LOCAL.x509.KeyUsage(True, False, False, False, False, True, True, False, False),
                         critical=True)
          .sign(ca_key, algorithm=None))
    LOCAL.write(credential_dir / 'ca-key.der', LOCAL.key_der(ca_key))
    LOCAL.write(credential_dir / 'ca.der', ca.public_bytes(LOCAL.serialization.Encoding.DER))
    public_keys, pins = {}, {}
    for node in NODES:
        signing = LOCAL.ed25519.Ed25519PrivateKey.generate()
        public_keys[node] = list(signing.public_key().public_bytes(
            LOCAL.serialization.Encoding.Raw, LOCAL.serialization.PublicFormat.Raw))
        LOCAL.write(credential_dir / f'signing-{node}.der', LOCAL.signing_der(signing))
        tls_key = LOCAL.ed25519.Ed25519PrivateKey.generate()
        dns = f'node-{node}.localhost'
        cert = (LOCAL.x509.CertificateBuilder().subject_name(LOCAL.name(dns)).issuer_name(ca_name)
                .public_key(tls_key.public_key()).serial_number(LOCAL.x509.random_serial_number())
                .not_valid_before(now - LOCAL.datetime.timedelta(minutes=5))
                .not_valid_after(now + LOCAL.datetime.timedelta(days=30))
                .add_extension(LOCAL.x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(LOCAL.x509.SubjectAlternativeName([LOCAL.x509.DNSName(dns)]),
                               critical=False)
                .add_extension(LOCAL.x509.ExtendedKeyUsage(
                    [LOCAL.ExtendedKeyUsageOID.SERVER_AUTH, LOCAL.ExtendedKeyUsageOID.CLIENT_AUTH]),
                    critical=False)
                .add_extension(LOCAL.x509.KeyUsage(True, False, False, False, False, False, False, False, False),
                               critical=True)
                .sign(ca_key, algorithm=None))
        der = cert.public_bytes(LOCAL.serialization.Encoding.DER)
        pins[node] = list(hashlib.sha256(der).digest())
        LOCAL.write(credential_dir / f'node-{node}.der', der)
        LOCAL.write(credential_dir / f'node-{node}-key.der', LOCAL.key_der(tls_key))
    LOCAL.save(credential_dir / 'secrets.json',
               {'subject': secrets.token_hex(32), 'raft': secrets.token_hex(32)})
    ensure_credentials(root)
    LOCAL.save(root / 'config' / 'ports.json', {'port_base': base})
    cluster = list(uuid.uuid4().bytes)
    for node in NODES:
        paths = []
        for group in GROUPS:
            peers = [{'id': n, 'address': f'127.0.0.1:{raft_port(base, group, n)}',
                      'public_key': public_keys[n],
                      'server_name': f'node-{n}.localhost', 'cert_sha256': pins[n]}
                     for n in NODES]
            path = root / 'config' / f'node-{node}-group-{group}.json'
            LOCAL.save(path, {
                'version': 1,
                'root': str(root / 'metadata'),
                'mode': 'create',
                'initialize': node == group,
                'cluster': cluster,
                'group': group,
                'incarnation': 1,
                'node': node,
                'preferred_leader': group,
                'raft_timing': {'heartbeat_ms': 250, 'election_min_ms': 1500,
                                'nonpreferred_election_min_ms': 2750, 'election_max_ms': 3000},
                'listen': peers[node - 1]['address'],
                'signing_key_der': str(credential_dir / f'signing-{node}.der'),
                'tls': {'roots_der': [str(credential_dir / 'ca.der')],
                        'chain_der': [str(credential_dir / f'node-{node}.der')],
                        'key_der': str(credential_dir / f'node-{node}-key.der')},
                'peers': peers,
            })
            paths.append(str(path))
        LOCAL.save(root / 'config' / f'node-{node}-manifest.json',
                   {'version': 2, 'control_group': CONTROL_GROUP,
                    'project_groups': PROJECT_GROUPS, 'groups': paths})
    LOCAL.save(root / 'config' / 'identity.json',
               {'port_base': base, 'node_ids': list(NODES), 'control_group': CONTROL_GROUP,
                'project_groups': PROJECT_GROUPS, 'deployment_id': str(uuid.uuid4()),
                'listener_sha256': listener_hash(root, base)})
    write_json(root / 'state.json', {
        'version': 1, 'phase': 'prepared', 'port_base': base, 'pids': [],
        'apply_attempted': False, 'applied': False, 'started_at': None,
    })
    print(f'prepared OpenRaft Multi-Raft root: {root}')
    print(f'  control group {CONTROL_GROUP}; project groups {PROJECT_GROUPS}')


def flip_to_open(root):
    """Restart every group with explicit open mode; never initialize again."""
    for node in NODES:
        for group in GROUPS:
            path = root / 'config' / f'node-{node}-group-{group}.json'
            cfg = LOCAL.load(path)
            cfg.update(mode='open', initialize=False)
            LOCAL.replace(path, cfg)


def wait_healthy(base):
    deadline = time.monotonic() + START_TIMEOUT
    pending = set(NODES)
    while pending and time.monotonic() < deadline:
        pending = {node for node in pending if not http_healthy(http_port(base, node))}
        if pending:
            time.sleep(0.1)
    if pending:
        raise RuntimeError(
            f'HTTP health check did not answer on node(s) {sorted(pending)} within {START_TIMEOUT}s')


def wait_leaders(root, base):
    """Read-only wait until every node observes a leader for all three groups."""
    secret = load_secrets(root)['subject']
    headers = signed_headers(secret, ADMIN)
    deadline = time.monotonic() + START_TIMEOUT
    last = None
    while time.monotonic() < deadline:
        observations = []
        for node in NODES:
            status, body = http_json(http_port(base, node), '/openraft/status', headers)
            if status != 200 or not isinstance(body, dict):
                observations = None
                break
            groups = body.get('local_observations')
            if not isinstance(groups, list) or len(groups) != len(GROUPS):
                observations = None
                break
            if any(group.get('leader_id') is None for group in groups):
                observations = None
                break
            observations.append(groups)
        if observations is not None:
            return observations
        last = observations
        time.sleep(0.1)
    raise RuntimeError(
        f'each node did not report three group leaders within {START_TIMEOUT}s; last={last!r}')


def wait_control(base):
    deadline = time.monotonic() + START_TIMEOUT
    pending = set(NODES)
    while pending and time.monotonic() < deadline:
        for node in list(pending):
            status, body = http_json(http_port(base, node), '/v1/status')
            if status == 200 and isinstance(body, dict) and 'billing/ledger' in body.get('databases', {}):
                pending.discard(node)
        if pending:
            time.sleep(0.1)
    if pending:
        raise RuntimeError(
            f'control-plane cache did not report billing/ledger on node(s) {sorted(pending)} '
            f'within {START_TIMEOUT}s; the manifests may not have been applied')


def apply_yaml(root, base, path, node=1):
    secret = load_secrets(root)['subject']
    body = json.dumps({'yaml': Path(path).read_text()}).encode()
    request = urllib.request.Request(
        f'http://127.0.0.1:{http_port(base, node)}/v1/apply', data=body,
        headers={'Content-Type': 'application/json', **signed_headers(secret, ADMIN)},
        method='POST')
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status, data = response.status, response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(
            f'POST /v1/apply failed with HTTP {error.code}: '
            f'{error.read().decode(errors="replace")}') from error
    except OSError as error:
        raise RuntimeError(f'POST /v1/apply failed: {error}') from error
    if not 200 <= status < 300:
        raise RuntimeError(f'POST /v1/apply failed with HTTP {status}: {data.decode(errors="replace")}')
    return status, data


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
    print(f'  Raft        {", ".join(f"g{g}/n{n} 127.0.0.1:{raft_port(base, g, n)}" for g in GROUPS for n in NODES)}')
    print(f'  S3 target   {S3_TARGET}')
    print(f'  iSCSI target {ISCSI_TARGET}')
    print(f'  admin subject {ADMIN} (signed with the subject secret)')


def print_credentials(root):
    print(f'credentials: {root / "credentials.json"} (mode 0600)')
    print(f'  PostgreSQL user / S3 access key / iSCSI CHAP name: {USER}')
    print(f'  subject: {MEMBER_SUBJECT}')
    print(f'  PostgreSQL database: billing/ledger')


def cmd_prepare(args):
    os.umask(0o077)
    validate_base(args.port_base)
    root = resolve_root(args.root, create=True)
    if not DAEMON.is_file():
        raise RuntimeError(f'missing daemon binary {DAEMON}; build it first with:\n  {BUILD_HINT}')
    try:
        root.mkdir(mode=0o700, exist_ok=False)
    except FileExistsError as error:
        raise RuntimeError(f'root {root} already exists; --prepare requires a fresh directory') from error
    except OSError as error:
        raise RuntimeError(
            f'cannot create root {root}: {error}; the parent directory must already exist') from error
    provision(root, args.port_base)
    print('prepared; not started')


def cmd_start(args):
    os.umask(0o077)
    validate_base(args.port_base)
    root = resolve_root(args.root, create=True)
    if not DAEMON.is_file():
        raise RuntimeError(f'missing daemon binary {DAEMON}; build it first with:\n  {BUILD_HINT}')
    if not root.exists():
        root.mkdir(mode=0o700, exist_ok=True)
        provision(root, args.port_base)
    with cluster_lock(root):
        state = read_state(root)
        if state is None:
            data_dirs = [root / 'data' / f'node-{node}' for node in NODES]
            if any(path.exists() for path in data_dirs):
                raise RuntimeError(
                    f'{root} contains node data but no state.json; refusing to guess how it was '
                    f'created. Inspect {root} and remove it only if you are certain it is disposable')
            provision(root, args.port_base)
            state = read_state(root)
        recorded_base = state.get('port_base')
        if recorded_base != args.port_base:
            raise RuntimeError(
                f'state.json records --port-base {recorded_base} for this root; the data dirs and '
                f'peer map are bound to it. Rerun with --port-base {recorded_base}, or use a new --root')
        phase = state['phase']
        pids = state.get('pids', [])
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
                    f'processes really are gone. Data dirs are reused; manifests are not re-applied')
            print('WARNING: stale "running" state; continuing because --force was given.',
                  file=sys.stderr)
        elif phase == 'stopped':
            alive = [pid for pid in pids if pid_alive(pid)]
            if alive:
                if not args.force:
                    raise RuntimeError(
                        f'state.json says phase "stopped" but recorded PID(s) {alive} are alive. '
                        f'Inspect with `{sys.argv[0]} status --root {root}`; use stop to terminate '
                        f'them, or rerun start with --force only after confirming they are this cluster')
                print(f'WARNING: "stopped" state has live recorded PID(s) {alive}; continuing '
                      f'with --force.', file=sys.stderr)
            flip_to_open(root)
        elif phase != 'prepared':
            if not args.force:
                raise RuntimeError(
                    f'state.json phase {phase!r} is not a settled state. Inspect '
                    f'`{sys.argv[0]} status --root {root}` and the logs, then rerun start with '
                    f'--force if the recorded processes are gone. No data is wiped or re-initialised')
            print(f'WARNING: previous phase {phase!r}; continuing because --force was given.',
                  file=sys.stderr)

        busy = busy_ports(args.port_base)
        if busy:
            details = '\n'.join(f'  {kind} node {node} port {port}: {error}'
                                for kind, node, port, error in busy)
            raise RuntimeError(f'refusing to start; derived ports are already in use:\n{details}')

        applied = bool(state.get('applied'))
        apply_attempted = bool(state.get('apply_attempted'))
        should_apply = not applied and not apply_attempted
        if apply_attempted and not applied:
            print('WARNING: a previous signed /v1/apply was attempted but never confirmed. It is '
                  'not retried automatically; inspect the control plane with status/logs.',
                  file=sys.stderr)
        record = {
            'version': 1, 'phase': 'starting', 'port_base': args.port_base, 'pids': [],
            'apply_attempted': apply_attempted, 'applied': applied, 'started_at': None,
        }
        write_json(root / 'state.json', record)
        env = daemon_env(root)
        children = []
        try:
            for node in NODES:
                log_path = root / 'logs' / f'node-{node}.log'
                log = log_path.open('ab', buffering=0)
                try:
                    child = subprocess.Popen(
                        node_command(root, args.port_base, node), env=env,
                        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                finally:
                    log.close()
                children.append(child)
                record['pids'] = [process.pid for process in children]
                write_json(root / 'state.json', record)
            print(f'started pids {record["pids"]}; waiting for readiness', flush=True)
            wait_healthy(args.port_base)
            observations = wait_leaders(root, args.port_base)
            print('every node observes a leader for control group 1 and data groups 2 and 3')
            for node, groups in zip(NODES, observations):
                summary = ', '.join(f'g{g["group"]}=leader {g["leader_id"]}' for g in groups)
                print(f'  node {node}: {summary}')
            if should_apply:
                record['apply_attempted'] = True
                write_json(root / 'state.json', record)
                print(f'applying {PLATFORM} once with a signed admin subject', flush=True)
                status, body = apply_yaml(root, args.port_base, PLATFORM)
                print(f'  /v1/apply HTTP {status}: {body.decode(errors="replace").strip()}')
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
    print('\nCluster running (openraft Multi-Raft runtime)')
    print(f'root: {root}')
    print(f'port base: {args.port_base}')
    print_endpoints(args.port_base)
    print_credentials(root)
    if should_apply:
        print(f'control plane: applied {PLATFORM.name} on first start')
    else:
        print('control plane: existing data reused; examples/platform.yaml was not re-applied')


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
            if not pid:
                print(f'node {node}: intentionally stopped (no recorded PID)')
                continue
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


def launch_node(root, base, node, env):
    """Spawn one daemon, appending to its log, in its own session."""
    log_path = root / 'logs' / f'node-{node}.log'
    with log_path.open('ab', buffering=0) as log:
        return subprocess.Popen(
            node_command(root, base, node), env=env,
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)


def wait_node_http(base, node, timeout=START_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if http_healthy(http_port(base, node)):
            return
        time.sleep(0.1)
    raise RuntimeError(f'node {node} HTTP did not answer on port {http_port(base, node)} '
                       f'within {timeout}s; inspect its log')


def _running_state(root):
    """Read state.json and refuse unless the cluster is meant to be up."""
    if not (root / 'state.json').exists():
        raise RuntimeError(f'{root} has no state.json; nothing to operate on')
    state = read_state(root)
    if state.get('phase') not in ('running', 'starting'):
        raise RuntimeError(f'{root} is not a running cluster (phase {state.get("phase")!r}); '
                           f'use start first')
    pids = state.get('pids') or []
    if len(pids) != 3:
        raise RuntimeError(f'state.json records {len(pids)} PIDs, expected 3; manual diagnosis required')
    return state


def _signal_one(root, state, node, sig):
    """Signal one recorded node and wait for it to exit; returns its old PID."""
    base = state['port_base']
    pids = list(state['pids'])
    pid = pids[node - 1]
    if not pid or not pid_alive(pid):
        print(f'node {node}: no live recorded PID')
        return None
    if not is_our_daemon(pid, root, node):
        raise RuntimeError(
            f'refusing to signal PID {pid}: its command line is not this root\'s node-{node} '
            f'daemon. Nothing was signalled')
    os.kill(pid, sig)
    deadline = time.monotonic() + STOP_TIMEOUT
    while pid_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    if pid_alive(pid):
        raise RuntimeError(f'node {node} PID {pid} did not exit after {sig.name} within '
                           f'{STOP_TIMEOUT}s; not killing it')
    print(f'node {node} PID {pid}: {sig.name} delivered; exited')
    return pid


def _bring_up(root, state, node):
    """Start one recorded-down node and wait for its health endpoint."""
    base = state['port_base']
    pids = list(state['pids'])
    if pids[node - 1] and pid_alive(pids[node - 1]):
        raise RuntimeError(f'node {node} is already running (PID {pids[node - 1]})')
    if port_open(http_port(base, node)):
        raise RuntimeError(f'HTTP port {http_port(base, node)} for node {node} '
                           f'is already bound by another process')
    child = launch_node(root, base, node, daemon_env(root))
    pids[node - 1] = child.pid
    state.update(pids=pids, down=[n for n, pid in zip(NODES, pids) if not pid])
    write_json(root / 'state.json', state)
    print(f'node {node}: started PID {child.pid}; waiting for readiness', flush=True)
    try:
        wait_node_http(base, node)
    except BaseException:
        if child.poll() is None:
            child.terminate()
        raise
    print(f'node {node} PID {child.pid}: HTTP ready')


def cmd_stop_node(args):
    os.umask(0o077)
    root = resolve_root(args.root, create=False)
    sig = signal.SIGKILL if args.signal == 'KILL' else signal.SIGTERM
    with cluster_lock(root):
        state = _running_state(root)
        _signal_one(root, state, args.node, sig)
        pids = list(state['pids'])
        pids[args.node - 1] = None
        state.update(pids=pids, down=[n for n, pid in zip(NODES, pids) if not pid])
        write_json(root / 'state.json', state)
        print(f'node {args.node} is intentionally down; the other nodes stay recorded and running')
    return 0


def cmd_start_node(args):
    os.umask(0o077)
    root = resolve_root(args.root, create=False)
    with cluster_lock(root):
        state = _running_state(root)
        _bring_up(root, state, args.node)
    return 0


def cmd_restart_node(args):
    """Stop one node (TERM by default, KILL on request) and start it again."""
    os.umask(0o077)
    root = resolve_root(args.root, create=False)
    sig = signal.SIGKILL if args.signal == 'KILL' else signal.SIGTERM
    with cluster_lock(root):
        state = _running_state(root)
        _signal_one(root, state, args.node, sig)
        pids = list(state['pids'])
        pids[args.node - 1] = None
        state.update(pids=pids, down=[n for n, pid in zip(NODES, pids) if not pid])
        write_json(root / 'state.json', state)
        _bring_up(root, state, args.node)
    return 0


def cmd_status(args):
    root = resolve_root(args.root, create=False)
    state = read_state(root) if (root / 'state.json').exists() else None
    print(f'root: {root}')
    if state is None:
        print('phase: not started (no state.json)')
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
    down = set(state.get('down') or [])
    problems = []
    print(f'phase: {phase}')
    print(f'port base: {base}')
    alive = {}
    if pids:
        if len(pids) != 3:
            problems.append(f'{phase} state records {len(pids)} PIDs, expected 3')
        for node, pid in zip(NODES, pids):
            if not pid:
                alive[node] = False
                print(f'node {node}: intentionally stopped (no recorded PID)')
                continue
            present = pid_alive(pid)
            alive[node] = present
            print(f'pid {pid} (node {node}): {"alive" if present else "absent"}; '
                  f'cmdline={pid_cmdline(pid)}')
            if present and not is_our_daemon(pid, root, node):
                problems.append(f'PID {pid} is alive but is not this root\'s node-{node} daemon')

    http = {node: http_healthy(http_port(base, node)) for node in NODES}
    pg = {node: port_open(pg_port(base, node)) for node in NODES}
    s3 = {node: port_open(s3_port(base, node)) for node in NODES}
    iscsi = {node: port_open(iscsi_port(base, node)) for node in NODES}
    raft = {(group, node): port_open(raft_port(base, group, node))
            for group in GROUPS for node in NODES}
    print('HTTP:      ', {f'node {node}': 'ok' if ok else 'down' for node, ok in http.items()})
    print('PostgreSQL:', {f'node {node}': 'open' if ok else 'closed' for node, ok in pg.items()})
    print('S3:        ', {f'node {node}': 'open' if ok else 'closed' for node, ok in s3.items()})
    print('iSCSI:     ', {f'node {node}': 'open' if ok else 'closed' for node, ok in iscsi.items()})
    print('Raft:      ', {f'g{group}/n{node}': 'open' if ok else 'closed'
                         for (group, node), ok in raft.items()})
    if (root / 'credentials.json').exists():
        print(f'credentials: {root / "credentials.json"} (user {USER})')

    if phase == 'running':
        if len(pids) != 3:
            problems.append('running state does not record three PIDs')
        for node, pid in zip(NODES, pids):
            if node in down:
                continue
            if not alive.get(node):
                problems.append(f'recorded PID {pid} for node {node} is absent')
        for node in NODES:
            if node in down:
                continue
            for kind, ok, port in (('HTTP', http[node], http_port(base, node)),
                                   ('PostgreSQL', pg[node], pg_port(base, node)),
                                   ('S3', s3[node], s3_port(base, node)),
                                   ('iSCSI', iscsi[node], iscsi_port(base, node))):
                if not ok:
                    problems.append(f'{kind} port {port} is not ready')
        if state.get('applied'):
            for node in NODES:
                status, body = http_json(http_port(base, node), '/v1/status')
                if status != 200 or not isinstance(body, dict) or 'billing/ledger' not in body.get('databases', {}):
                    problems.append(f'node {node} does not report billing/ledger in its control-plane cache')
    elif phase == 'stopped':
        for node, pid in zip(NODES, pids):
            if alive.get(node):
                problems.append(f'stopped state still has live recorded PID {pid}')
        busy = busy_ports(base)
        for kind, node, port, error in busy:
            problems.append(f'stopped state still has a listener on {kind} port {port} ({error})')
    elif phase == 'prepared':
        for kind, node, port, error in busy_ports(base):
            problems.append(f'prepared state has a listener on {kind} port {port} ({error})')
    else:
        problems.append(f'phase {phase!r} is not a settled state; run start --force or stop')

    if problems:
        print('INCONSISTENT:')
        for problem in problems:
            print(f'  - {problem}')
        return 1
    print('consistent')
    return 0


def cmd_apply(args):
    root = resolve_root(args.root, create=False)
    state = read_state(root)
    if state is None or state.get('phase') not in ('running', 'starting'):
        raise RuntimeError(f'{root} is not a started cluster; run start first')
    status, body = apply_yaml(root, state['port_base'], args.file, args.node)
    print(f'POST /v1/apply HTTP {status}: {body.decode(errors="replace").strip()}')
    return 0


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

    prepare = sub.add_parser('prepare', help='provision a fresh root without starting it')
    prepare.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    prepare.add_argument('--port-base', type=int, default=DEFAULT_PORT_BASE)

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

    apply = sub.add_parser('apply', help='signed POST /v1/apply of a YAML file')
    apply.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    apply.add_argument('--file', type=Path, required=True)
    apply.add_argument('--node', type=int, default=1, choices=NODES)

    logs = sub.add_parser('logs', help='tail each node log')
    logs.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    logs.add_argument('--lines', type=int, default=40)

    for name, help_text in (
            ('restart-node', 'restart one node; --signal KILL sends SIGKILL first'),
            ('stop-node', 'stop one node and leave it down; --signal KILL is allowed'),
            ('start-node', 'start a node that stop-node left down')):
        node_parser = sub.add_parser(name, help=help_text)
        node_parser.add_argument('--root', type=Path, default=DEFAULT_ROOT)
        node_parser.add_argument('--node', type=int, required=True, choices=NODES)
        if name != 'start-node':
            node_parser.add_argument('--signal', choices=('TERM', 'KILL'), default='TERM',
                                     help='signal used to stop the node (default TERM)')

    args = parser.parse_args()
    if getattr(args, 'lines', 1) < 1:
        parser.error('--lines must be at least 1')
    if args.command == 'prepare':
        cmd_prepare(args)
    elif args.command == 'start':
        cmd_start(args)
    elif args.command == 'stop':
        return cmd_stop(args)
    elif args.command == 'status':
        return cmd_status(args)
    elif args.command == 'apply':
        return cmd_apply(args)
    elif args.command == 'restart-node':
        return cmd_restart_node(args)
    elif args.command == 'stop-node':
        return cmd_stop_node(args)
    elif args.command == 'start-node':
        return cmd_start_node(args)
    else:
        return cmd_logs(args)


if __name__ == '__main__':
    try:
        sys.exit(main() or 0)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f'Error: {error}', file=sys.stderr)
        sys.exit(1)
