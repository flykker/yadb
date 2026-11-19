#!/usr/bin/env python3
"""Provision and supervise a disposable local OpenRaft cluster."""
import argparse
import datetime
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
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
    subprocess.run(['cargo', 'build', '--offline', '--locked', '-j', '2', '-p', 'yadbd', '--features', 'openraft-storage'], cwd=REPO, env=env, check=True)
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
                         # `heartbeat_ms` is the append budget, not a tuning knob:
                         # OpenRaft wraps `append_entries` in
                         # `C::timeout(heartbeat_interval, ...)` and does not retry a
                         # miss, so the replication session for that peer ends. At
                         # 250 ms a fresh TCP + mTLS handshake could consume the
                         # whole budget under load and a healthy cluster abandoned
                         # appends the follower had already written. 750 ms keeps
                         # the budget above the transport cost; the daemon requires
                         # `election_min_ms >= 3 x heartbeat_ms`.
                         # Test/tuning knob: add `'snapshot_after_logs': N`
                         # to this dict to snapshot (and InstallSnapshot to a
                         # follower) after N log entries instead of the daemon
                         # default of 5000. It is omitted so the provisioned
                         # config stays byte-identical to earlier versions.
                         'raft_timing': {'heartbeat_ms': 750, 'election_min_ms': 2250,
                                         'nonpreferred_election_min_ms': 3000, 'election_max_ms': 3500},
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
            '--openraft-config', str(root / 'config' / f'node-{node}-manifest.json'),
            '--peers', peers, '--admin-subject', OPERATOR]


def run(root):
    os.umask(0o077)
    root = root.resolve(strict=True)
    with (root / 'supervisor.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        statepath = root / 'state.json'
        state = load(statepath)
        if state['phase'] not in ('prepared', 'stopped'):
            raise RuntimeError('Previous run incomplete; inspect artifacts manually. No automatic retry.')
        http_base = deployment_ports(root)
        check_ports(http_base)
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
                        req = urllib.request.Request(f'http://127.0.0.1:{http_port(http_base, node)}/openraft/status', headers={'x-yadb-subject': OPERATOR, 'x-yadb-subject-signature': signature})
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
        for path in ('/healthz', '/openraft/status'):
            request = urllib.request.Request(f'http://127.0.0.1:{http_port(http_base, node)}{path}', headers={'x-yadb-subject': subject, 'x-yadb-subject-signature': signature})
            with urllib.request.urlopen(request, timeout=5) as response:
                print(f'node {node} {path}:', response.read().decode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prepare_parser = sub.add_parser('prepare')
    prepare_parser.add_argument('--root', type=Path, help='new deployment directory (parent must exist)')
    prepare_parser.add_argument('--port-base', type=int, default=DEFAULT_HTTP_BASE,
                                help='first HTTP port; Raft listeners use base+10000..base+10008')
    for cmd in ('run', 'verify', 'status'):
        sub.add_parser(cmd).add_argument('root', type=Path)
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.root, args.port_base)
    elif args.command == 'run':
        run(args.root)
    elif args.command == 'status':
        if not status(args.root.resolve(strict=True)):
            sys.exit(1)
    else:
        verify(args.root.resolve(strict=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f'Error: {error}', file=sys.stderr)
        sys.exit(1)
