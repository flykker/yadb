#!/usr/bin/env python3
"""Provision and supervise a disposable, experimental local OpenRaft cluster."""
import argparse
import contextlib
import datetime
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path, PurePosixPath
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
import urllib.request
import uuid

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

REPO = Path(__file__).resolve().parent.parent
BASE = Path('/data/data/com.foxdebug.acodefree/cache/opencode')
DEFAULT_HTTP_BASE = 8420
RAFT_OFFSET = 10000
# The single operator identity this disposable deployment provisions. It is the
# only entry on every daemon's --admin-subject allowlist and the subject that
# run/verify sign with, so those two uses cannot drift apart.
OPERATOR = 'local-operator'


def http_port(http_base, node):
    return http_base + node - 1


def raft_port(http_base, group, node):
    return http_base + RAFT_OFFSET + (group - 1) * 3 + node - 1


def ports(http_base):
    """All HTTP and Raft listener ports owned by one deployment."""
    return ([http_port(http_base, node) for node in range(1, 4)]
            + [raft_port(http_base, group, node) for group in range(1, 4) for node in range(1, 4)])


def deployment_ports(root):
    """Verify local deployment identity before selecting client ports."""
    config = Path(root) / 'config'
    ports_path = config / 'ports.json'
    identity_path = config / 'identity.json'
    if not ports_path.exists() and not identity_path.exists():
        print('Warning: legacy deployment; cluster identity cannot be verified (using port base 8420).', file=sys.stderr)
        return DEFAULT_HTTP_BASE
    if not identity_path.exists():
        raise ValueError('cluster identity verification failed: config/identity.json is missing')
    if not ports_path.exists():
        raise ValueError('cluster identity verification failed: config/ports.json is missing')
    try:
        identity = load(identity_path)
        base = identity['http_base']
        if type(base) is not int or not 1024 <= base <= 55527:
            raise ValueError('cluster identity verification failed: identity port base is out of range')
        recorded_base = load(ports_path)['http_base']
        if type(recorded_base) is not int or recorded_base != base:
            raise ValueError('cluster identity verification failed: port bases disagree')
        if identity['node_ids'] != [1, 2, 3] or not isinstance(identity['deployment_id'], str) or not identity['deployment_id']:
            raise ValueError('cluster identity verification failed: invalid identity')
        if listener_hash(root, base) != identity['listener_sha256']:
            raise ValueError('cluster identity verification failed: listener hash mismatch')
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError(f'cluster identity verification failed: invalid or unreadable configuration ({error})') from error
    return base


def listener_hash(root, http_base):
    """Hash HTTP listeners and Raft listeners/peer addresses from group configs."""
    config = Path(root) / 'config'
    listeners = {'http': [f'127.0.0.1:{http_port(http_base, node)}' for node in range(1, 4)],
                 'raft': []}
    for node in range(1, 4):
        for group in range(1, 4):
            cfg = load(config / f'node-{node}-group-{group}.json')
            listeners['raft'].append({'node': cfg['node'], 'group': cfg['group'],
                                      'listen': cfg['listen'],
                                      'peers': [{'id': peer['id'], 'address': peer['address']} for peer in cfg['peers']]})
    canonical = json.dumps(listeners, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(canonical).hexdigest()


def write(path, data):
    path = Path(path)
    with path.open('xb') as f:
        f.write(data)
    os.chmod(path, 0o600)


def save(path, obj):
    write(path, (json.dumps(obj, indent=2) + '\n').encode())


def replace(path, obj):
    tmp = path.with_suffix('.tmp')
    save(tmp, obj)
    os.replace(tmp, path)


def load(path):
    return json.loads(path.read_text())


def key_der(key):
    return key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def signing_der(key):
    # ring's strict Ed25519 PKCS#8 parser requires the v2 public-key field.
    seed = key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
                             serialization.NoEncryption())
    public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return bytes.fromhex('3051020101300506032b657004220420') + seed + bytes.fromhex('812100') + public


def name(cn):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def check_ports(http_base):
    sockets = []
    try:
        for port in ports(http_base):
            s = socket.socket()
            # Ignore TIME_WAIT left by a clean shutdown; SO_REUSEADDR still
            # refuses to bind over a live listener.
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sockets.append(s)
            s.bind(('127.0.0.1', port))
    finally:
        for s in sockets:
            s.close()


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
    return [part.decode(errors='replace') for part in raw.split(b'\0') if part]


def http_health(port):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}/healthz', timeout=2) as response:
            return response.status == 200
    except OSError:
        return False


def port_open(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=1):
            return True
    except OSError:
        return False


def status(root):
    """Report whether state.json agrees with the processes and ports actually present."""
    root = Path(root)
    aliases = {str(root), str(root.resolve(strict=True))}
    root = root.resolve(strict=True)
    state = load(root / 'state.json')
    phase = state.get('phase')
    http_base = deployment_ports(root)
    recorded = []
    if state.get('supervisor_pid') is not None:
        recorded.append(state['supervisor_pid'])
    recorded.extend(state.get('pids', []))
    alive = {pid: pid_alive(pid) for pid in recorded}
    cmdlines = {pid: pid_cmdline(pid) for pid in recorded}
    http = {port: http_health(port) for port in (http_port(http_base, n) for n in range(1, 4))}
    raft = {port: port_open(port) for port in (raft_port(http_base, g, n) for g in range(1, 4) for n in range(1, 4))}

    problems = []
    if phase == 'running':
        if state.get('supervisor_pid') is None or len(state.get('pids', [])) != 3:
            problems.append('running state does not record one supervisor and three children')
        for pid, present in alive.items():
            if not present:
                problems.append(f'recorded PID {pid} is absent')
                continue
            words = cmdlines[pid] or []
            if pid == state.get('supervisor_pid'):
                if 'yadb-local.py' not in ' '.join(words) or 'run' not in words or not aliases.intersection(words):
                    problems.append(f'PID {pid} does not look like this root supervisor')
            elif not any(
                any(str(Path(alias) / 'bin/yadbd') in word for word in words)
                for alias in aliases
            ):
                problems.append(f'PID {pid} does not look like this root child')
        for port, ok in http.items():
            if not ok:
                problems.append(f'HTTP port {port} is not healthy')
        for port, ok in raft.items():
            if not ok:
                problems.append(f'Raft port {port} is not accepting connections')
    elif phase == 'stopped':
        for pid, present in alive.items():
            if present:
                problems.append(f'stopped state still has live recorded PID {pid}')
        for port, ok in {**http, **raft}.items():
            if ok:
                problems.append(f'stopped state still has a listener on port {port}')
    elif phase == 'prepared':
        if any(alive.values()):
            problems.append('prepared state has live recorded processes')
        for port, ok in {**http, **raft}.items():
            if ok:
                problems.append(f'prepared state has a listener on port {port}')
    else:
        problems.append(f'phase {phase!r} is not startable without manual diagnosis')

    print(f'root: {root}')
    print(f'phase: {phase}')
    for pid in recorded:
        print(f'pid {pid}: {"alive" if alive[pid] else "absent"}; cmdline={cmdlines[pid]}')
    print('http:', {port: 'ok' if ok else 'down' for port, ok in http.items()})
    print('raft:', {port: 'open' if ok else 'closed' for port, ok in raft.items()})
    if problems:
        print('INCONSISTENT:')
        for problem in problems:
            print(f'  - {problem}')
        return False
    print('consistent')
    return True


def validate_http_base(http_base):
    """Reject a base that cannot host all twelve listeners below port 65536."""
    if not 1024 <= http_base <= 55527:
        raise ValueError('--port-base must leave room for the HTTP and Raft listeners')


def prepare(root=None, http_base=DEFAULT_HTTP_BASE):
    os.umask(0o077)
    validate_http_base(http_base)
    check_ports(http_base)
    if root is None:
        root = BASE / ('yadb-local-' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + str(uuid.uuid4()))
    else:
        root = Path(root)
        if root.name in ('', '.', '..'):
            raise ValueError('Deployment root must name a new directory')
        # Resolve only the existing parent: resolving the final component would
        # follow a symlink rather than letting mkdir reject it atomically.
        root = root.parent.resolve(strict=True) / root.name
    root.mkdir(mode=0o700)
    print('Deployment root:', root, flush=True)
    for directory in ('credentials', 'config', 'metadata', 'data', 'logs', 'bin'):
        (root / directory).mkdir(mode=0o700)
    print('Building feature-enabled daemon offline; artifacts remain on failure.', flush=True)
    env = os.environ.copy()
    env['CARGO_INCREMENTAL'] = '0'
    subprocess.run(['cargo', 'build', '--offline', '--locked', '-j', '2', '-p', 'yadbd', '--features', 'experimental-openraft-storage'], cwd=REPO, env=env, check=True)
    binary = root / 'bin/yadbd'
    shutil.copyfile(REPO / 'target/debug/yadbd', binary)
    binary.chmod(0o700)
    cred = root / 'credentials'
    ca_key = ed25519.Ed25519PrivateKey.generate()
    now = datetime.datetime.utcnow()
    ca_name = name('yadb local ' + str(uuid.uuid4()))
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - datetime.timedelta(minutes=5)).not_valid_after(now + datetime.timedelta(days=30))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.KeyUsage(True, False, False, False, False, True, True, False, False), critical=True)
          .sign(ca_key, algorithm=None))
    write(cred / 'ca-key.der', key_der(ca_key))
    write(cred / 'ca.der', ca.public_bytes(serialization.Encoding.DER))
    public_keys, pins = {}, {}
    for node in range(1, 4):
        signing = ed25519.Ed25519PrivateKey.generate()
        public_keys[node] = list(signing.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw))
        write(cred / f'signing-{node}.der', signing_der(signing))
        tls_key = ed25519.Ed25519PrivateKey.generate()
        dns = f'node-{node}.localhost'
        cert = (x509.CertificateBuilder().subject_name(name(dns)).issuer_name(ca_name)
                .public_key(tls_key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now - datetime.timedelta(minutes=5)).not_valid_after(now + datetime.timedelta(days=30))
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(x509.SubjectAlternativeName([x509.DNSName(dns)]), critical=False)
                .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
                .add_extension(x509.KeyUsage(True, False, False, False, False, False, False, False, False), critical=True)
                .sign(ca_key, algorithm=None))
        der = cert.public_bytes(serialization.Encoding.DER)
        pins[node] = list(hashlib.sha256(der).digest())
        write(cred / f'node-{node}.der', der)
        write(cred / f'node-{node}-key.der', key_der(tls_key))
    save(cred / 'secrets.json', {'subject': secrets.token_hex(32), 'raft': secrets.token_hex(32)})
    save(root / 'config' / 'ports.json', {'http_base': http_base})
    cluster = list(uuid.uuid4().bytes)
    for node in range(1, 4):
        paths = []
        for group in range(1, 4):
            peers = [{'id': n, 'address': f'127.0.0.1:{raft_port(http_base, group, n)}',
                      'public_key': public_keys[n], 'server_name': f'node-{n}.localhost', 'cert_sha256': pins[n]} for n in range(1, 4)]
            path = root / 'config' / f'node-{node}-group-{group}.json'
            save(path, {'version': 1, 'root': str(root / 'metadata'), 'mode': 'create', 'initialize': node == group,
                         'cluster': cluster, 'group': group, 'incarnation': 1, 'node': node,
                         'preferred_leader': group,
                         'raft_timing': {'heartbeat_ms': 250, 'election_min_ms': 1500,
                                         'nonpreferred_election_min_ms': 2750, 'election_max_ms': 3000},
                        'listen': peers[node-1]['address'], 'signing_key_der': str(cred / f'signing-{node}.der'),
                        'tls': {'roots_der': [str(cred / 'ca.der')], 'chain_der': [str(cred / f'node-{node}.der')],
                                'key_der': str(cred / f'node-{node}-key.der')}, 'peers': peers})
            paths.append(str(path))
        save(root / 'config' / f'node-{node}-manifest.json', {'version': 2, 'control_group': 1, 'project_groups': {'1': 2, '2': 3}, 'groups': paths})
    save(root / 'config' / 'identity.json', {'http_base': http_base, 'node_ids': [1, 2, 3],
         'deployment_id': str(uuid.uuid4()), 'listener_sha256': listener_hash(root, http_base)})
    save(root / 'state.json', {'phase': 'prepared'})
    print('Prepared; not started. Run:', sys.executable, __file__, 'run', root)


def cleanup(children, statepath, ready, interrupted):
    errors = []
    completed = False
    def record(child, action, error):
        message = f'PID {child.pid} {action}: {error}'
        errors.append(message)
        print(message, file=sys.stderr)

    try:
        for child in children:
            try:
                if child.poll() is None:
                    child.send_signal(signal.SIGINT)
            except OSError as error:
                record(child, 'signal failed', error)
        deadline = time.monotonic() + 30
        for child in children:
            try:
                child.wait(timeout=max(0, deadline-time.monotonic()))
            except subprocess.TimeoutExpired:
                record(child, 'did not stop; inspect manually', 'timeout')
            except OSError as error:
                record(child, 'wait failed', error)
        completed = True
    finally:
        exit_codes = []
        for child in children:
            try:
                exit_codes.append(child.poll())
            except OSError as error:
                record(child, 'poll failed', error)
                exit_codes.append(None)
        clean = completed and ready and interrupted and len(children) == 3 and not errors and all(code == 0 for code in exit_codes)
        state = {'phase': 'stopped' if clean else 'incomplete', 'pids': [c.pid for c in children], 'exit_codes': exit_codes}
        if errors:
            state['cleanup_errors'] = errors
        replace(statepath, state)
        if not clean:
            print('Incomplete run; artifacts preserved, no automatic restart.', file=sys.stderr)
    return clean


def node_command(root, http_base, node, peers):
    """Daemon argv for one node.

    OpenRaft refuses to start without a non-empty --admin-subject allowlist, so
    the operator identity is part of the launch contract rather than an optional
    extra. The subject secret itself is passed through the environment.
    """
    return [str(root / 'bin/yadbd'), '--listen', f'127.0.0.1:{http_port(http_base, node)}',
            '--node-id', str(node), '--data-dir', str(root / 'data' / f'node-{node}'),
            '--metadata-runtime', 'openraft',
            '--experimental-openraft-config', str(root / 'config' / f'node-{node}-manifest.json'),
            '--peers', peers, '--admin-subject', OPERATOR]


def run(root):
    os.umask(0o077)
    root = root.resolve(strict=True)
    marker = root / '.yadb-backup-in-progress'
    with (root / 'supervisor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # The marker must be read after the lock is held. Checking it before
        # taking the lock leaves a window: a `run` paused between check and lock
        # would start after a backup had written the marker, and the copy would
        # race a live process. Backup holds no supervisor lock, so this ordering
        # is what makes "stopped" mean stopped for the whole copy.
        try:
            marker.lstat()
        except FileNotFoundError:
            pass
        else:
            raise RuntimeError(f'Deployment root is under an offline backup ({marker}); '
                               'finish or explicitly discard the backup and remove the marker.')
        statepath = root / 'state.json'
        state = load(statepath)
        if state['phase'] not in ('prepared', 'stopped'):
            raise RuntimeError('Previous run incomplete; inspect artifacts manually. No automatic retry.')
        http_base = deployment_ports(root)
        check_ports(http_base)
        # `logs/` is derived output, not deployment state: a backup artifact
        # excludes it deliberately, so a restored root has none. Creating it here
        # is what lets an artifact be started at all; it holds no information.
        (root / 'logs').mkdir(mode=0o700, exist_ok=True)
        # Fail closed even if interrupted while switching restart configurations.
        replace(statepath, {'phase': 'starting', 'supervisor_pid': os.getpid()})
        if state['phase'] == 'stopped':
            for node in range(1, 4):
                for group in range(1, 4):
                    path = root / 'config' / f'node-{node}-group-{group}.json'
                    cfg = load(path)
                    cfg.update(mode='open', initialize=False)
                    replace(path, cfg)
        secret = load(root / 'credentials/secrets.json')
        env = os.environ.copy()
        env.update(YADB_SUBJECT_SECRET=secret['subject'], YADB_RAFT_TOKEN=secret['raft'])
        peers = ','.join(f'{n}=127.0.0.1:{http_port(http_base, n)}' for n in range(1, 4))
        children = []
        interrupted = False
        ready = False
        def stop(_sig, _frame):
            nonlocal interrupted
            interrupted = True
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        try:
            for node in range(1, 4):
                log = (root / 'logs' / f'node-{node}.log').open('ab', buffering=0)
                try:
                    child = subprocess.Popen(node_command(root, http_base, node, peers),
                        env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                finally:
                    log.close()
                children.append(child)
                replace(statepath, {'phase': 'starting', 'supervisor_pid': os.getpid(), 'pids': [c.pid for c in children]})
            print('Processes started:', [c.pid for c in children], 'Use verify; Ctrl-C to stop.', flush=True)
            deadline = time.monotonic() + 30
            signature = hmac.new(secret['subject'].encode(), OPERATOR.encode(), hashlib.sha256).hexdigest()
            while not interrupted and all(c.poll() is None for c in children) and time.monotonic() < deadline:
                try:
                    observations = []
                    for node in range(1, 4):
                        req = urllib.request.Request(f'http://127.0.0.1:{http_port(http_base, node)}/experimental/openraft/status', headers={'x-yadb-subject': OPERATOR, 'x-yadb-subject-signature': signature})
                        with urllib.request.urlopen(req, timeout=1) as response:
                            observations.append(json.load(response)['local_observations'])
                    if all(len(groups) == 3 and all(g.get('leader_id') is not None for g in groups) for groups in observations):
                        ready = True
                        break
                except (OSError, ValueError, KeyError):
                    pass
                time.sleep(0.25)
            if not ready:
                raise RuntimeError('Startup did not yield three observed leaders per node; inspect artifacts')
            replace(statepath, {'phase': 'running', 'supervisor_pid': os.getpid(), 'pids': [c.pid for c in children]})
            print('Each node reports three leaders; this is not a replication test.', flush=True)
            while not interrupted and all(c.poll() is None for c in children):
                time.sleep(0.25)
        finally:
            clean = cleanup(children, statepath, ready, interrupted)
        if not clean:
            raise RuntimeError('Cluster did not stop cleanly')


def verify(root):
    secret = load(root / 'credentials/secrets.json')['subject']
    http_base = deployment_ports(root)
    subject = OPERATOR
    signature = hmac.new(secret.encode(), subject.encode(), hashlib.sha256).hexdigest()
    for node in range(1, 4):
        for path in ('/healthz', '/experimental/openraft/status'):
            request = urllib.request.Request(f'http://127.0.0.1:{http_port(http_base, node)}{path}', headers={'x-yadb-subject': subject, 'x-yadb-subject-signature': signature})
            with urllib.request.urlopen(request, timeout=5) as response:
                print(f'node {node} {path}:', response.read().decode())


class BackupError(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.code = code


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_write(path, data):
    with Path(path).open('xb') as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def backup_copy(source, target, relative, files):
    """Refuse ambiguous filesystem objects rather than silently omitting them."""
    info = source.lstat()
    if stat.S_ISDIR(info.st_mode):
        target.mkdir(mode=stat.S_IMODE(info.st_mode))
        for child in sorted(source.iterdir()):
            if (child.name in ('.yadb-backup-in-progress', 'supervisor.lock', 'logs')
                    or child.name.endswith('.tmp') or child.name.startswith('.l2s.')):
                continue
            backup_copy(child, target / child.name, relative / child.name, files)
        os.chmod(target, stat.S_IMODE(info.st_mode))
        sync_dir(target)
    elif stat.S_ISREG(info.st_mode):
        # Reject all multiply linked files: an external link cannot be ruled out
        # from a single path without trusting a concurrently mutable tree.
        if info.st_nlink != 1:
            raise BackupError(f'hard-linked file refused: {source}', 4)
        digest = hashlib.sha256()
        size = 0
        fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as input_file, target.open('xb') as output:
            actual = os.fstat(input_file.fileno())
            if not stat.S_ISREG(actual.st_mode) or (actual.st_dev, actual.st_ino, actual.st_nlink) != (info.st_dev, info.st_ino, 1):
                raise BackupError(f'file changed during copy: {source}', 4)
            while True:
                block = input_file.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
                size += len(block)
                output.write(block)
            os.fchmod(output.fileno(), stat.S_IMODE(info.st_mode))
            output.flush()
            os.fsync(output.fileno())
        files.append({'path': relative.as_posix(), 'size': size, 'mode': stat.S_IMODE(info.st_mode), 'sha256': digest.hexdigest()})
    else:
        raise BackupError(f'symlink or special file refused: {source}', 4)


def cluster_identity(root, base):
    """Return what can actually be verified about this deployment's identity.

    A root prepared by an older tooling version has no `config/identity.json`, so
    there is no recorded `deployment_id` to report and inventing one would claim
    the artifact belongs to a deployment that never existed. The verifiable facts
    are still there: the cluster UUID and node set in every replica descriptor,
    and the listeners derived from the group configs. Those are reported, and
    `identity_source` says which of the two situations this is.
    """
    root = Path(root)
    cluster_ids = set()
    nodes = set()
    for replica in sorted((root / 'metadata').iterdir()):
        version, group, node, _voters = json.loads((replica / 'replica.json').read_text())
        if version != 1:
            raise BackupError(f'unsupported replica descriptor version in {replica}', 4)
        cluster_ids.add(tuple(group['cluster']))
        nodes.add(node)
    if len(cluster_ids) != 1:
        raise BackupError('replica descriptors disagree on the cluster identity', 4)
    identity_path = root / 'config' / 'identity.json'
    if identity_path.exists():
        identity = load(identity_path)
        if identity['node_ids'] != sorted(nodes):
            raise BackupError('recorded node ids disagree with the replica descriptors', 4)
        if identity['listener_sha256'] != listener_hash(root, base):
            raise BackupError('recorded listener hash disagrees with the group configs', 4)
        return {'identity_source': 'provisioned', 'deployment_id': identity['deployment_id'],
                'listener_sha256': identity['listener_sha256'], 'node_ids': sorted(nodes),
                'cluster_id': list(next(iter(cluster_ids))), 'http_base': base}
    return {'identity_source': 'legacy', 'deployment_id': None,
            'listener_sha256': listener_hash(root, base), 'node_ids': sorted(nodes),
            'cluster_id': list(next(iter(cluster_ids))), 'http_base': base}


def backup(root, dest):
    os.umask(0o077)
    created = False
    markers = []
    marker_data = b''
    complete = None
    try:
        try:
            root = root.resolve(strict=True)
            base = deployment_ports(root)
            identity = cluster_identity(root, base)
            state = load(root / 'state.json')
            if not isinstance(state, dict) or state.get('phase') != 'stopped':
                raise BackupError('deployment phase is not stopped', 2)
            if not isinstance(state.get('pids', []), list) or any(type(pid) is not int or pid <= 0 for pid in state.get('pids', [])) or (state.get('supervisor_pid') is not None and (type(state['supervisor_pid']) is not int or state['supervisor_pid'] <= 0)):
                raise BackupError('invalid recorded PIDs', 2)
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise BackupError(f'invalid deployment root or identity: {error}', 2) from error
        dest = dest.parent.resolve(strict=True) / dest.name
        if dest == root or root in dest.parents:
            raise BackupError('destination must be outside the deployment root', 2)
        marker = root / '.yadb-backup-in-progress'
        if marker.exists() or marker.is_symlink():
            raise BackupError(f'backup marker already exists: {marker}', 2)
        # Holding this lock closes the window where run has checked the marker
        # but has not yet acquired its own supervisor lock.
        with (root / 'supervisor.lock').open('a') as supervisor:
            try:
                fcntl.flock(supervisor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise BackupError('supervisor lock is held', 2) from error
            # Diagnostics belong on stderr: stdout carries the artifact path and
            # digest, and a caller must not have to parse around status output.
            with contextlib.redirect_stdout(sys.stderr):
                consistent = status(root)
            if not consistent:
                raise BackupError('deployment status reports problems', 2)
            for port in ports(base):
                if port_open(port):
                    raise BackupError(f'listener on port {port}', 2)
            # Created only after every precondition that needs no destination has
            # passed, so a refusal leaves no directory that looks like a backup.
            try:
                dest.mkdir(mode=0o700)
            except FileExistsError as error:
                raise BackupError(f'destination already exists: {dest}', 2) from error
            created = True
            (dest / 'payload').mkdir(mode=0o700)
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            marker_data = (json.dumps({'destination': str(dest), 'timestamp_utc': timestamp}) + '\n').encode()
            markers = [marker] + [root / 'data' / f'node-{node}' / marker.name for node in identity['node_ids']]
            for path in markers:
                durable_write(path, marker_data)
                sync_dir(path.parent)
            metadata = root / 'metadata'
            replicas = sorted(metadata.iterdir())
            if len(replicas) != 9 or any(not stat.S_ISDIR(path.lstat().st_mode) for path in replicas):
                raise BackupError('expected nine replica directories in metadata', 4)
            locks = []
            try:
                for path in replicas:
                    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                    locks.append(fd)
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError as error:
                        raise BackupError(f'replica lock is held: {path}', 2) from error
            finally:
                for fd in locks:
                    os.close(fd)
            verdicts = []
            for path in replicas:
                result = subprocess.run([str(root / 'bin/yadbd'), '--verify-replica', str(path)], capture_output=True, text=True)
                if result.returncode != 0:
                    raise BackupError(f'replica validation failed: {path}: {result.stderr.strip()}', 4)
                try:
                    verdict = json.loads(result.stdout)
                    required = {'group', 'node', 'first_index', 'last_index', 'last_purged', 'applied',
                                'snapshot', 'entry_count', 'torn_tail', 'restorable', 'reason',
                                'membership_differs_from_descriptor'}
                    if not isinstance(verdict, dict) or not required.issubset(verdict):
                        raise ValueError('missing verdict fields')
                    if verdict['restorable'] is not True:
                        raise ValueError(f"not restorable: {verdict['reason']}")
                except (ValueError, TypeError) as error:
                    raise BackupError(f'invalid replica verdict: {path}: {error}', 4) from error
                if verdict['membership_differs_from_descriptor'] is not None:
                    raise BackupError(f'replica membership disagrees with descriptor: {path}', 4)
                if verdict['torn_tail'] is not True and verdict['torn_tail'] is not False:
                    raise BackupError(f'invalid replica verdict: {path}: torn_tail is not a boolean', 4)
                if verdict['torn_tail']:
                    # A torn tail is copied byte for byte, and the next open
                    # truncates it. So publishing this artifact would silently
                    # drop the final frame, and the frame's bytes cannot say
                    # whether it was an interrupted append or a length field
                    # corrupted upward (docs/multiraft-verification.md). A clean
                    # shutdown never leaves one, so the honest answer is to
                    # refuse: the operator starts the deployment once (the opener
                    # repairs the tail deterministically) and stops it again.
                    raise BackupError(
                        f'replica has a torn log tail: {path}; start and cleanly stop the '
                        'deployment so the opener resolves it before taking a backup', 4)
                verdicts.append(verdict)
            files = []
            for name in ('config', 'metadata', 'data', 'credentials', 'bin', 'state.json'):
                backup_copy(root / name, dest / 'payload' / name, Path(name), files)
            sync_dir(dest / 'payload')
            manifest = {'version': 1, 'source_root': str(root),
                        'identity_source': identity['identity_source'],
                        'deployment_id': identity['deployment_id'],
                        'cluster_id': identity['cluster_id'],
                        'listener_sha256': identity['listener_sha256'], 'timestamp_utc': timestamp,
                        'node_ids': identity['node_ids'], 'blob_coverage': 'full', 'replicas': verdicts, 'files': files}
            data = (json.dumps(manifest, indent=2) + '\n').encode()
            digest = hashlib.sha256(data).hexdigest()
            durable_write(dest / 'manifest.json', data)
            durable_write(dest / 'manifest.sha256', (digest + '\n').encode())
            sync_dir(dest)
            complete = dest / 'COMPLETE'
            durable_write(complete, (digest + '\n').encode())
            sync_dir(dest)
            for path in reversed(markers):
                path.unlink()
                sync_dir(path.parent)
            print(f'Backup: {dest}\nSHA-256: {digest}')
            return 0
    except (BackupError, OSError, ValueError, KeyError, TypeError) as error:
        code = error.code if isinstance(error, BackupError) else 5
        if created:
            if complete is not None:
                try:
                    complete.unlink(missing_ok=True)
                    sync_dir(dest)
                except OSError:
                    pass
            # A destination that exists must be unambiguous: it either carries
            # COMPLETE or INCOMPLETE, never neither. Markers are restored here
            # too, because a failure after the last marker was removed must not
            # leave a root that looks safe to start.
            for path in markers:
                try:
                    if not path.exists() and not path.is_symlink():
                        durable_write(path, marker_data)
                        sync_dir(path.parent)
                except OSError:
                    pass
            try:
                durable_write(dest / 'INCOMPLETE', (str(error) + '\n').encode())
                sync_dir(dest)
            except OSError as record_error:
                print(f'Could not record INCOMPLETE: {record_error}', file=sys.stderr)
            print(f'Backup retained at {dest}: {error}', file=sys.stderr)
        else:
            print(f'Backup refused: {error}', file=sys.stderr)
        return code


def read_artifact(artifact):
    """Prove the artifact is complete and self-consistent, without trusting it.

    Returns the decoded manifest and its digest. The digest is not
    authenticated: `--expected-sha256` is how an operator supplies the value
    from a trusted channel, and a replaced manifest plus matching hash is
    undetectable here.
    """
    artifact = Path(artifact)
    incomplete = artifact / 'INCOMPLETE'
    if incomplete.exists() or incomplete.is_symlink():
        raise BackupError(f'artifact is marked INCOMPLETE: {incomplete.read_text().strip()}', 3)
    complete = artifact / 'COMPLETE'
    try:
        recorded = complete.read_text().strip()
    except OSError as error:
        raise BackupError(f'artifact has no COMPLETE record: {error}', 3) from error
    if len(recorded) != 64 or any(c not in '0123456789abcdef' for c in recorded):
        raise BackupError(f'COMPLETE does not hold one SHA-256 digest: {complete}', 4)
    data = (artifact / 'manifest.json').read_bytes()
    if len(data) > 64 * 1024 * 1024:
        raise BackupError('manifest is implausibly large', 4)
    digest = hashlib.sha256(data).hexdigest()
    stated = (artifact / 'manifest.sha256').read_text().strip()
    if not stated == digest == recorded:
        raise BackupError('manifest.json, manifest.sha256 and COMPLETE disagree', 4)
    try:
        manifest = json.loads(data)
    except ValueError as error:
        raise BackupError(f'manifest is not JSON: {error}', 4) from error
    if not isinstance(manifest, dict) or manifest.get('version') != 1:
        raise BackupError('unsupported manifest version', 4)
    return manifest, digest


def verified_payload(artifact, manifest):
    """Return the declared files after proving the payload is exactly them."""
    payload = Path(artifact) / 'payload'
    declared = {}
    entries = manifest.get('files')
    if not isinstance(entries, list):
        raise BackupError('manifest does not list payload files', 4)
    for entry in entries:
        if not isinstance(entry, dict):
            raise BackupError('manifest file entry is not an object', 4)
        relative = entry.get('path')
        if not isinstance(relative, str):
            raise BackupError('manifest file entry has no path', 4)
        pure = PurePosixPath(relative)
        if pure.is_absolute() or not pure.parts or '..' in pure.parts or relative in declared:
            raise BackupError(f'unsafe or duplicate payload path: {relative}', 4)
        if type(entry.get('size')) is not int or entry['size'] < 0:
            raise BackupError(f'payload entry has no valid size: {relative}', 4)
        if not isinstance(entry.get('sha256'), str) or len(entry['sha256']) != 64:
            raise BackupError(f'payload entry has no valid hash: {relative}', 4)
        if type(entry.get('mode')) is not int or not 0 <= entry['mode'] <= 0o7777:
            raise BackupError(f'payload entry has no valid mode: {relative}', 4)
        declared[relative] = entry
    if not declared:
        raise BackupError('manifest declares no payload files', 4)
    # The payload must be exactly the manifest: a missing file is a truncated
    # artifact, and an extra one is content nobody validated or hashed.
    actual = set()
    for path in payload.rglob('*'):
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode):
            raise BackupError(f'artifact holds a symlink or special file: {path}', 4)
        if info.st_nlink != 1:
            raise BackupError(f'artifact holds a hard-linked file: {path}', 4)
        actual.add(path.relative_to(payload).as_posix())
    if actual != set(declared):
        missing = sorted(set(declared) - actual)[:3]
        extra = sorted(actual - set(declared))[:3]
        raise BackupError(f'payload does not match manifest; missing={missing} extra={extra}', 4)
    for relative, entry in declared.items():
        # Stream: a payload can hold multi-gigabyte volume files, and reading one
        # into memory would fail on a healthy artifact.
        digest = hashlib.sha256()
        size = 0
        with (payload / relative).open('rb') as input_file:
            while True:
                block = input_file.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
                size += len(block)
        if size != entry['size'] or digest.hexdigest() != entry['sha256']:
            raise BackupError(f'payload file does not match its recorded hash: {relative}', 4)
    return declared


def verify_recorded_replicas(manifest):
    """Re-check the verdicts backup recorded, so a bad artifact cannot pass."""
    replicas = manifest.get('replicas')
    if not isinstance(replicas, list) or len(replicas) != 9:
        raise BackupError('manifest must record nine replica verdicts', 4)
    seen = set()
    for verdict in replicas:
        if not isinstance(verdict, dict):
            raise BackupError('manifest replica verdict is not an object', 4)
        if verdict.get('restorable') is not True:
            raise BackupError(f"artifact records a replica that is not restorable: {verdict.get('reason')}", 4)
        if verdict.get('torn_tail') is not False:
            raise BackupError('artifact records a replica with a torn log tail', 4)
        if verdict.get('membership_differs_from_descriptor') is not None:
            raise BackupError('artifact records a membership that disagrees with its descriptor', 4)
        group = verdict.get('group')
        group_id = group.get('group') if isinstance(group, dict) else None
        node = verdict.get('node')
        if type(group_id) is not int or group_id not in (1, 2, 3) or type(node) is not int or node not in (1, 2, 3):
            raise BackupError('artifact records an unknown group or node', 4)
        seen.add((group_id, node))
    if len(seen) != 9:
        raise BackupError('artifact does not cover all nine replicas', 4)


def restore(artifact, target, expected_sha256=None):
    """Restore a COMPLETE artifact in place, over a destroyed root.

    In place only: `experimental_openraft.rs` binds the absolute client root and
    every absolute metadata path into `.yadb-openraft-runtime.json`, so a copy at
    another path is refused by the daemon as a marker mismatch. That refusal is
    what stops a second copy of one identity from running, so this command does
    not rewrite paths to defeat it.
    """
    os.umask(0o077)
    created = False
    finished = False
    markers = []
    digest = ''
    timestamp = ''
    try:
        artifact = artifact.resolve(strict=True)
        manifest, digest = read_artifact(artifact)
        if expected_sha256 is not None and expected_sha256.strip().lower() != digest:
            raise BackupError('artifact digest does not match --expected-sha256', 2)
        declared = verified_payload(artifact, manifest)
        verify_recorded_replicas(manifest)
        source_root = manifest.get('source_root')
        if not isinstance(source_root, str) or not source_root:
            raise BackupError('manifest does not record its source root', 4)
        absolute = target.parent.resolve(strict=True) / target.name
        if absolute != Path(source_root).resolve():
            raise BackupError(
                f'restore is in place only: the artifact was taken from {source_root}, not '
                f'{absolute}. The runtime marker binds absolute paths, so a copy at another '
                'path is a different deployment, not a restore.', 2)
        if absolute.exists():
            if not absolute.is_dir() or any(absolute.iterdir()):
                raise BackupError(f'target exists and is not empty: {absolute}', 2)
        else:
            absolute.mkdir(mode=0o700)
        created = True
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        record = (f'{digest}\n{timestamp}\n').encode()
        # Both markers go down before the first byte is copied. RESTORE_INCOMPLETE
        # records that the root is mid-restore; the backup marker makes the daemon
        # and `run` refuse to start it, so a crash cannot leave a half-copied root
        # that looks startable.
        markers = [absolute / 'RESTORE_INCOMPLETE', absolute / '.yadb-backup-in-progress']
        for path in markers:
            durable_write(path, record)
        sync_dir(absolute)
        directories = {absolute}
        for relative in sorted(declared):
            entry = declared[relative]
            destination = absolute / relative
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            directories.add(destination.parent)
            source_fd = os.open(artifact / 'payload' / relative, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                info = os.fstat(source_fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise BackupError(f'artifact payload file is not a plain file: {relative}', 4)
                output_fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                try:
                    # Hash the bytes actually written, not the bytes the manifest
                    # claims, so a payload swapped after verification is caught.
                    written_hash = hashlib.sha256()
                    size = 0
                    while True:
                        block = os.read(source_fd, 1024 * 1024)
                        if not block:
                            break
                        written_hash.update(block)
                        size += len(block)
                        view = memoryview(block)
                        while view:
                            view = view[os.write(output_fd, view):]
                    if size != entry['size'] or written_hash.hexdigest() != entry['sha256']:
                        raise BackupError(f'copied bytes do not match the manifest: {relative}', 4)
                    os.fchmod(output_fd, entry['mode'])
                    os.fsync(output_fd)
                finally:
                    os.close(output_fd)
            finally:
                os.close(source_fd)
        for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
            sync_dir(directory)
        metadata = absolute / 'metadata'
        replicas = sorted(metadata.iterdir())
        if len(replicas) != 9 or any(not stat.S_ISDIR(path.lstat().st_mode) for path in replicas):
            raise BackupError('restored root does not hold nine replica directories', 4)
        # Validate the bytes that are now on disk, not the verdicts backup recorded.
        for path in replicas:
            result = subprocess.run([str(absolute / 'bin/yadbd'), '--verify-replica', str(path)],
                                    capture_output=True, text=True)
            if result.returncode != 0:
                raise BackupError(f'restored replica failed validation: {path}: {result.stderr.strip()}', 4)
            verdict = json.loads(result.stdout)
            if (verdict.get('restorable') is not True or verdict.get('torn_tail') is not False
                    or verdict.get('membership_differs_from_descriptor') is not None):
                raise BackupError(f'restored replica is not clean: {path}: {verdict.get("reason")}', 4)
        # The artifact's PIDs belong to the dead deployment. Keeping them would let
        # a recycled PID make `status` report a running deployment that is not one.
        temporary = absolute / 'state.json.restore'
        with temporary.open('xb') as output:
            output.write(json.dumps({'phase': 'stopped'}).encode() + b'\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, absolute / 'state.json')
        sync_dir(absolute)
        for path in markers:
            path.unlink()
            sync_dir(absolute)
        durable_write(absolute / 'RESTORE_COMPLETE', record)
        sync_dir(absolute)
        finished = True
        print(f'Restored: {absolute}\nSHA-256: {digest}')
        return 0
    except (BackupError, OSError, ValueError, KeyError, TypeError) as error:
        code = error.code if isinstance(error, BackupError) else 5
        if created:
            if finished:
                try:
                    (target.parent.resolve(strict=True) / target.name / 'RESTORE_COMPLETE').unlink()
                except OSError:
                    pass
            # A root whose restore failed must never look complete or startable.
            for path in markers:
                try:
                    if not path.exists() and not path.is_symlink():
                        durable_write(path, (f'{digest}\n{timestamp}\n').encode())
                except OSError:
                    pass
            print(f'Restore retained at {target}: {error}', file=sys.stderr)
        else:
            print(f'Restore refused: {error}', file=sys.stderr)
        return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prepare_parser = sub.add_parser('prepare')
    prepare_parser.add_argument('--root', type=Path, help='new deployment directory (parent must exist)')
    prepare_parser.add_argument('--port-base', type=int, default=DEFAULT_HTTP_BASE,
                                help='first HTTP port; Raft listeners use base+10000..base+10008')
    for cmd in ('run', 'verify', 'status'):
        sub.add_parser(cmd).add_argument('root', type=Path)
    backup_parser = sub.add_parser('backup')
    backup_parser.add_argument('root', type=Path)
    backup_parser.add_argument('dest', type=Path)
    restore_parser = sub.add_parser('restore')
    restore_parser.add_argument('artifact', type=Path)
    restore_parser.add_argument('target', type=Path)
    restore_parser.add_argument('--expected-sha256', help='digest obtained from a trusted channel')
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.root, args.port_base)
    elif args.command == 'run':
        run(args.root)
    elif args.command == 'status':
        if not status(args.root.resolve(strict=True)):
            sys.exit(1)
    elif args.command == 'backup':
        sys.exit(backup(args.root, args.dest))
    elif args.command == 'restore':
        sys.exit(restore(args.artifact, args.target, args.expected_sha256))
    else:
        verify(args.root.resolve(strict=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f'Error: {error}', file=sys.stderr)
        sys.exit(1)
