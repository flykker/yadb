#!/usr/bin/env python3
"""Three Firecracker microVMs, one yadb daemon each, on a host bridge.

Each node is a separate kernel, a separate TCP/IP stack and a separate root
filesystem, so this exercises what a container cannot: a guest page cache that
disappears when the VMM is killed, and network behaviour that is not a
userspace relay on loopback. It is still one physical machine, so a host panic,
a disk failure and a power-supply loss remain out of scope, and the host file's
own durability depends on Firecracker honouring the guest's flush (this harness
sets `cache_type: Writeback`, which fsyncs the backing file).

Scenarios, in the order they run:

  * the cluster comes up and every group elects a leader on every node;
  * SQL, S3 and block writes are acknowledged through a non-leader and read
    back byte-exactly through every node;
  * all three VMMs are killed at once, restarted from their own disks, and the
    writes acknowledged before the cut must still be served locally;
  * one of three VMMs is killed: the survivors keep acknowledging writes, and
    the killed node restarts from its own disk and catches up.

Usage:
    python3 scripts/yadb-microvm.py [--port-base 23600] [--only up|services|powercut|lose|all]

The script re-execs itself under passwordless sudo, because tap devices, the
bridge and Firecracker need root. See scripts/README-microvm.md for the one-time
rootfs preparation.
"""
import argparse
import datetime
import hashlib
import hmac
import importlib.util
import json
import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('yadb_dev', REPO / 'scripts/yadb-openraft-dev.py')
DEV = importlib.util.module_from_spec(spec)
spec.loader.exec_module(DEV)

NODES = (1, 2, 3)
GUEST_ROOT = Path('/var/yadb')
GUEST_DATA = GUEST_ROOT / 'data'
STORE = Path(os.environ.get('YADB_VMSTORE', '/home/flykker/vmstore'))
KERNEL = STORE / 'vmlinux-6.18.62'
ROOTFS = STORE / 'rootfs.ext4'

# One /24 for the whole fixture. The host bridge takes .1 and node N takes .1N.
SUBNET = '172.30.0'
GATEWAY = f'{SUBNET}.1'
NODE_IP = {n: f'{SUBNET}.1{n}' for n in NODES}
VCPU = 2
MEM_MIB = 1536

# The writes the service gate acknowledges once and every later scenario
# checks against, so a power cut is judged by data written before it.
GATE_NOTE = 'vm-gate'
PG_NOTE = 'pg-gate'
BLOCK_PAYLOAD = b'block-payload-over-microvm'
S3_BODY = b's3-object-over-microvm'
S3_KEY = '/invoices-2026/gate-object'
ROOTFS_MIB = 1200
DATA_MIB = 768


def run(*args, check=True, timeout=120, capture=True):
    return subprocess.run(list(args), text=True, timeout=timeout, check=check,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None)


def wait_for(description, predicate, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, ValueError, KeyError, urllib.error.URLError, RuntimeError):
            pass
        time.sleep(.3)
    raise AssertionError(f'{description}: timed out; last={last!r}')


def applied_index(row):
    """The applied log index from a `/openraft/status` row.

    The endpoint renders `last_applied` with `{:?}` — `Some(LogId { ... index: 5 })`
    or `None` — because it is an `Option<LogId>` and the shape is meant for an
    operator, not a parser. Extract the index rather than assuming a type.
    """
    match = re.search(r'index:\s*(\d+)', str(row.get('last_applied') or ''))
    return int(match.group(1)) if match else None


def s3_signed(secret, host, method, path, body=b'', extra=None):
    """SigV4 headers for one S3 wire request.

    The harness talks to the `--s3-listen` port with the real signature scheme
    rather than the daemon's internal `/s3/...` route, so "S3 works" means an
    ordinary S3 client works, not that an internal helper does.
    """
    now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    date = now[:8]
    headers = {'host': host, 'x-amz-date': now,
               'x-amz-content-sha256': hashlib.sha256(body).hexdigest()}
    headers.update({name.lower(): value for name, value in (extra or {}).items()})
    names = ';'.join(sorted(headers))
    uri, _, query = path.partition('?')
    encode = lambda value: urllib.parse.quote(value, safe='-_.~')
    canonical_path = '/'.join(encode(urllib.parse.unquote(part)) for part in uri.split('/'))
    pairs = [(encode(urllib.parse.unquote(k)), encode(urllib.parse.unquote(v)))
             for part in query.split('&') if part
             for k, _, v in [part.partition('=')]]
    canonical_query = '&'.join(f'{k}={v}' for k, v in sorted(pairs))
    canonical_headers = ''.join(f'{name}:{headers[name]}\n' for name in sorted(headers))
    canonical = (f'{method}\n{canonical_path}\n{canonical_query}\n'
                 f'{canonical_headers}\n{names}\n{headers["x-amz-content-sha256"]}')
    scope = f'{date}/us-east-1/s3/aws4_request'
    text = (f'AWS4-HMAC-SHA256\n{now}\n{scope}\n'
            f'{hashlib.sha256(canonical.encode()).hexdigest()}')
    key = ('AWS4' + secret).encode()
    for value in (date, 'us-east-1', 's3', 'aws4_request'):
        key = hmac.new(key, value.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, text.encode(), hashlib.sha256).hexdigest()
    headers['authorization'] = (f'AWS4-HMAC-SHA256 Credential={DEV.USER}/{scope}, '
                                f'SignedHeaders={names}, Signature={signature}')
    return headers


class Pg:
    """Minimal pgwire client: startup, cleartext password, simple query.

    The daemon's own protocol tests use a fuller client; this one exists so the
    VM gate can prove the database answers over the real PostgreSQL wire from
    another VM, not just through the HTTP query route. Only text-format rows are
    parsed, which is all a `SELECT` of integers and strings needs.
    """

    def __init__(self, host, port, user, database, secret, timeout=30):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        startup = struct.pack('!I', 196608)
        startup += f'user\0{user}\0database\0{database}\0\0'.encode()
        self.sock.sendall(struct.pack('!I', len(startup) + 4) + startup)
        assert self.packet() == (b'R', struct.pack('!I', 3)), 'expected cleartext auth'
        self.send(b'p', secret.encode() + b'\0')
        self.until_ready()

    def exact(self, count):
        data = b''
        while len(data) < count:
            chunk = self.sock.recv(count - len(data))
            if not chunk:
                raise RuntimeError('pgwire connection closed mid-message')
            data += chunk
        return data

    def send(self, kind, body):
        self.sock.sendall(kind + struct.pack('!I', len(body) + 4) + body)

    def packet(self):
        kind = self.exact(1)
        size, = struct.unpack('!I', self.exact(4))
        return kind, self.exact(size - 4)

    def until_ready(self):
        messages = []
        while True:
            kind, data = self.packet()
            messages.append((kind, data))
            if kind == b'Z':
                return messages

    def query(self, sql):
        self.send(b'Q', sql.encode() + b'\0')
        return self.until_ready()

    def rows(self, sql):
        out = self.query(sql)
        assert not any(kind == b'E' for kind, _ in out), out
        rows = []
        for kind, data in out:
            if kind != b'D':
                continue
            columns, = struct.unpack('!H', data[:2])
            data = data[2:]
            row = []
            for _ in range(columns):
                length, = struct.unpack('!i', data[:4])
                data = data[4:]
                row.append(None if length < 0 else data[:length].decode())
                data = data[max(length, 0):]
            rows.append(row)
        return rows

    def close(self):
        try:
            self.send(b'X', b'')
        finally:
            self.sock.close()


def eventually(description, probe, timeout=120):
    """Retry a probe that returns None on success or a failure description."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = probe()
        except (OSError, ValueError, KeyError, urllib.error.URLError) as error:
            last = f'{type(error).__name__}: {error}'
        if last is None:
            return
        time.sleep(.3)
    raise AssertionError(f'{description}: timed out; last failure: {last}')


def request(ip, port, method, path, body=b'', headers=None, timeout=60):
    """One signed request to a node's public listener, addressed by VM IP."""
    url = f'http://{ip}:{port}{path}'
    req = urllib.request.Request(url, data=body if method != 'GET' else None,
                                 headers=dict(headers or {}), method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


class Cluster:
    def __init__(self, root, base, keep=False):
        self.root = root
        self.base = base
        self.keep = keep
        self.tag = 'yfc' + uuid.uuid4().hex[:4]
        self.bridge = f'{self.tag}-br'
        self.tap = {n: f'{self.tag}-tap{n}' for n in NODES}
        self.procs = {}
        self.images = {}
        self.secret = None
        self.protocol_secret = None
        self.user_secret = None

    # ---- host networking -------------------------------------------------

    def link_up(self):
        run('ip', 'link', 'add', 'name', self.bridge, 'type', 'bridge')
        run('ip', 'addr', 'add', f'{GATEWAY}/24', 'dev', self.bridge)
        run('ip', 'link', 'set', self.bridge, 'up')
        for n in NODES:
            run('ip', 'tuntap', 'add', 'dev', self.tap[n], 'mode', 'tap')
            run('ip', 'link', 'set', self.tap[n], 'master', self.bridge)
            run('ip', 'link', 'set', self.tap[n], 'up')

    def link_down(self):
        """Remove what this run created; a link that never appeared is not an error.

        Cleanup also runs when `link_up` itself failed, so a missing device is
        expected there and must not hide the original failure.
        """
        errors = []
        for n in NODES:
            result = run('ip', 'link', 'del', 'dev', self.tap[n], check=False)
            if result.returncode and 'Cannot find device' not in result.stderr:
                errors.append(f'tap{n}: {result.stderr.strip()}')
        result = run('ip', 'link', 'del', 'dev', self.bridge, check=False)
        if result.returncode and 'Cannot find device' not in result.stderr:
            errors.append(f'bridge: {result.stderr.strip()}')
        if errors:
            raise RuntimeError('link cleanup: ' + '; '.join(errors))

    # ---- per-node disk ---------------------------------------------------

    def write_group_configs(self, target, node, mode_of):
        """Write one node's per-group Raft configs into `target`.

        `target` is the directory that becomes `/var/yadb` inside the guest:
        the image view at build time, or the mounted image at restart. `mode_of`
        answers `(mode, initialize)` per group so the disk itself decides
        whether a replica is created or reopened.
        """
        source = self.root / 'provision'
        for group in DEV.GROUPS:
            path = source / 'config' / f'node-{node}-group-{group}.json'
            cfg = json.loads(path.read_text())
            cfg['root'] = str(GUEST_ROOT / 'metadata')
            for peer in cfg['peers']:
                peer['address'] = (f"{NODE_IP[peer['id']]}:"
                                   f"{DEV.raft_port(self.base, group, peer['id'])}")
            cfg['listen'] = f"0.0.0.0:{DEV.raft_port(self.base, group, node)}"
            cfg['signing_key_der'] = str(GUEST_ROOT / 'credentials' / f'signing-{node}.der')
            cfg['tls'] = {
                'roots_der': [str(GUEST_ROOT / 'credentials' / 'ca.der')],
                'chain_der': [str(GUEST_ROOT / 'credentials' / f'node-{node}.der')],
                'key_der': str(GUEST_ROOT / 'credentials' / f'node-{node}-key.der'),
            }
            cfg['mode'], cfg['initialize'] = mode_of(group, cfg)
            (target / 'config' / f'node-{node}-group-{group}.json').write_text(
                json.dumps(cfg, indent=2))

    def prepare_restart(self, node):
        """Reopen the replicas that survived the kill instead of recreating them.

        A `create` config refuses to start when its replica directory is
        already on disk ("a replica may have started; ... use open mode"), and
        that is exactly the state a killed node comes back in. The disk decides,
        as the dev harness does: present means `open`, absent means `create`.
        Mounting is safe here only because the VMM is dead, so the guest is not
        holding the filesystem.
        """
        image = self.images[node]
        mountpoint = self.root / f'mnt-node-{node}'
        mountpoint.mkdir(exist_ok=True)
        run('mount', '-o', 'loop,rw', str(image), str(mountpoint))
        try:
            metadata = mountpoint / 'metadata'

            def mode_of(group, cfg):
                cluster = ''.join(f'{byte:02x}' for byte in cfg['cluster'])
                replica = metadata / f"{cluster}-{group}-{cfg['incarnation']}-{node}"
                if replica.is_dir():
                    return 'open', False
                return 'create', node == group

            self.write_group_configs(mountpoint, node, mode_of)
        finally:
            run('umount', str(mountpoint))

    def build_node_image(self, node):
        """Turn the provisioned host config into one node's writable disk.

        The daemon inside the guest sees `/var/yadb`, so every absolute path in
        the Raft config is rewritten from the host run directory, and every peer
        address from `127.0.0.1` to the peer VM's address. Nothing else about
        the config changes: same cluster id, same timings, same pins.
        """
        source = self.root / 'provision'
        view = self.root / 'guest' / f'node-{node}'
        for part in ('config', 'credentials', 'data', 'logs', 'metadata'):
            (view / part).mkdir(parents=True, exist_ok=True)
        for item in (source / 'credentials').iterdir():
            shutil.copy2(item, view / 'credentials' / item.name)
        shutil.copy2(source / 'credentials.json', view / 'credentials.json')
        (view / 'credentials.json').chmod(0o600)

        self.write_group_configs(view, node, lambda group, cfg: ('create', node == group))

        manifest = {
            'version': 2,
            'control_group': DEV.CONTROL_GROUP,
            'project_groups': DEV.PROJECT_GROUPS,
            'groups': [str(GUEST_ROOT / 'config' / f'node-{node}-group-{g}.json')
                       for g in DEV.GROUPS],
        }
        (view / 'config' / f'node-{node}-manifest.json').write_text(
            json.dumps(manifest, indent=2))

        peers = ','.join(f'{n}={NODE_IP[n]}:{DEV.http_port(self.base, n)}' for n in NODES)
        portals = ','.join(f'{NODE_IP[n]}:{DEV.iscsi_port(self.base, n)}' for n in NODES)
        argv = [
            '/usr/local/bin/yadbd',
            # The public listener must be the node's own address, not 0.0.0.0:
            # the runtime refuses to start unless `--peers[node]` and `--listen`
            # are the same socket address, so a wildcard listener would hide a
            # misconfigured peer table instead of failing at startup.
            '--listen', f'{NODE_IP[node]}:{DEV.http_port(self.base, node)}',
            '--node-id', str(node),
            '--data-dir', str(GUEST_DATA / f'node-{node}'),
            '--metadata-runtime', 'openraft',
            '--openraft-config', str(GUEST_ROOT / 'config' / f'node-{node}-manifest.json'),
            '--peers', peers,
            '--admin-subject', DEV.ADMIN,
            '--protocol-credentials', str(GUEST_ROOT / 'credentials.json'),
            '--pg-listen', f'0.0.0.0:{DEV.pg_port(self.base, node)}',
            '--s3-listen', f'0.0.0.0:{DEV.s3_port(self.base, node)}',
            '--s3-target', DEV.S3_TARGET,
            '--iscsi-listen', f'0.0.0.0:{DEV.iscsi_port(self.base, node)}',
            '--iscsi-portals', portals,
        ]
        start = view / 'start.sh'
        start.write_text(
            '#!/bin/sh\n'
            'set -e\n'
            f"export YADB_SUBJECT_SECRET='{self.secret}'\n"
            f"export YADB_RAFT_TOKEN='{self.protocol_secret}'\n"
            f'exec {" ".join(argv)}\n')
        start.chmod(0o755)

        image = self.root / f'disk-node-{node}.ext4'
        if image.exists():
            image.unlink()
        with open(image, 'wb') as handle:
            handle.truncate(DATA_MIB * 1024 * 1024)
        run('mkfs.ext4', '-q', '-F', '-d', str(view), '-L', f'yadb{node}', str(image))
        self.images[node] = image

    # ---- VM lifecycle ----------------------------------------------------

    def config_path(self, node):
        return self.root / f'vm-node-{node}.json'

    def write_vm_config(self, node):
        config = {
            'boot-source': {
                'kernel_image_path': str(KERNEL),
                'boot_args': (
                    'console=ttyS0 reboot=k panic=1 pci=off init=/init '
                    f'yadb.host=node-{node} yadb.ip={NODE_IP[node]} yadb.gw={GATEWAY}'),
            },
            'drives': [
                {'drive_id': 'root', 'path_on_host': str(ROOTFS),
                 'is_root_device': True, 'is_read_only': True},
                # Writeback is the only cache mode that fsyncs the backing file
                # when the guest flushes. Under the default `Unsafe` a guest
                # fsync never reaches the host, so a power-cut test would fail
                # for a harness reason rather than a daemon one.
                {'drive_id': 'data', 'path_on_host': str(self.images[node]),
                 'is_root_device': False, 'is_read_only': False, 'cache_type': 'Writeback'},
            ],
            'machine-config': {'vcpu_count': VCPU, 'mem_size_mib': MEM_MIB, 'smt': False},
            'network-interfaces': [
                {'iface_id': 'eth0', 'host_dev_name': self.tap[node],
                 'guest_mac': f'AA:FC:00:00:00:0{node}'},
            ],
        }
        self.config_path(node).write_text(json.dumps(config, indent=2))

    def start_vm(self, node):
        console_path = self.root / f'console-node-{node}.log'
        console = open(console_path, 'ab')
        sock = self.root / f'api-node-{node}.sock'
        if sock.exists():
            sock.unlink()
        proc = subprocess.Popen(
            ['firecracker', '--api-sock', str(sock),
             '--config-file', str(self.config_path(node))],
            stdout=console, stderr=subprocess.STDOUT, start_new_session=True)
        self.procs[node] = proc
        return proc

    def kill_vm(self, node):
        """Kill one VMM with SIGKILL — the power-cut case.

        The guest's RAM is freed immediately, so every dirty guest page the
        daemon had not flushed is lost. That is the property a container cannot
        show: a SIGKILLed process leaves its dirty pages in the host page cache.
        """
        proc = self.procs.pop(node, None)
        if proc is None:
            return
        if proc.poll() is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass

    def stop_vm(self, node):
        proc = self.procs.pop(node, None)
        if proc is None:
            return
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()

    # ---- readiness -------------------------------------------------------

    def status(self, node):
        status, _, body = request(
            NODE_IP[node], DEV.http_port(self.base, node), 'GET', '/openraft/status',
            headers=DEV.signed_headers(self.secret, DEV.ADMIN))
        if status != 200:
            raise RuntimeError(f'node {node} status HTTP {status}: {body[:200]!r}')
        return json.loads(body)

    def local_groups(self, node):
        return self.status(node)['local_observations']

    def group_rows(self, node):
        """Every group this node hosts, keyed by group id."""
        return {row['group']: row for row in self.local_groups(node)}

    def group_row(self, node, group):
        rows = self.group_rows(node)
        if group not in rows:
            raise RuntimeError(f'node {node} hosts no group {group}')
        return rows[group]

    def applied_map(self, node):
        return {group: applied_index(row) for group, row in self.group_rows(node).items()}

    def leader_of(self, node, group):
        for row in self.local_groups(node):
            if row['group'] == group:
                return row['leader_id']
        raise RuntimeError(f'node {node} does not know group {group}')

    def wait_leaders(self, timeout=180):
        def ready():
            for node in NODES:
                rows = self.local_groups(node)
                if len(rows) != len(DEV.GROUPS):
                    return False
                if any(row['leader_id'] is None for row in rows):
                    return False
            return True
        wait_for('every group elected a leader on every node', ready, timeout)

    def wait_applied(self, timeout=180, nodes=None):
        def caught_up():
            for node in (nodes or NODES):
                for row in self.local_groups(node):
                    if applied_index(row) != row['last_log_index']:
                        return False
            return True
        wait_for('every replica applied its complete log', caught_up, timeout)

    # ---- workload --------------------------------------------------------

    def sql(self, node, sql, timeout=120):
        body = json.dumps({'sql': sql, 'force_leader': False}).encode()
        headers = {'Content-Type': 'application/json',
                   **DEV.signed_headers(self.secret, DEV.MEMBER_SUBJECT)}
        status, _, out = request(NODE_IP[node], DEV.http_port(self.base, node), 'POST',
                                 '/db/billing/ledger/query', body, headers, timeout)
        return status, out

    def pg(self, node, timeout=30):
        """A pgwire connection to one node's PostgreSQL listener."""
        # The protocol user's password comes from credentials.json; the
        # signing secret is a different value and the listener closes the
        # connection when they are mixed up.
        return Pg(NODE_IP[node], DEV.pg_port(self.base, node), DEV.USER,
                  'billing/ledger', self.user_secret, timeout)

    def apply(self, node, yaml_text):
        body = json.dumps({'yaml': yaml_text}).encode()
        headers = {'Content-Type': 'application/json',
                   **DEV.signed_headers(self.secret, DEV.ADMIN)}
        status, _, out = request(NODE_IP[node], DEV.http_port(self.base, node), 'POST',
                                 '/v1/apply', body, headers, 120)
        if status != 200:
            raise RuntimeError(f'apply on node {node}: HTTP {status}: {out[:300]!r}')
        return json.loads(out)

    def s3_host(self, node):
        return f'{NODE_IP[node]}:{DEV.s3_port(self.base, node)}'

    def s3_put(self, node, key, data, timeout=120):
        headers = s3_signed(self.user_secret, self.s3_host(node), 'PUT', key, data)
        return request(NODE_IP[node], DEV.s3_port(self.base, node), 'PUT', key,
                       data, headers, timeout)

    def s3_get(self, node, key, timeout=120):
        headers = s3_signed(self.user_secret, self.s3_host(node), 'GET', key)
        return request(NODE_IP[node], DEV.s3_port(self.base, node), 'GET', key,
                       headers=headers, timeout=timeout)

    def block_write(self, node, volume, offset, data, timeout=120):
        headers = DEV.signed_headers(self.secret, DEV.MEMBER_SUBJECT)
        headers['Content-Type'] = 'application/octet-stream'
        return request(NODE_IP[node], DEV.http_port(self.base, node), 'PUT',
                       f'/block/billing/{volume}?offset={offset}', data, headers, timeout)

    def block_read(self, node, volume, offset, length, timeout=120):
        headers = DEV.signed_headers(self.secret, DEV.MEMBER_SUBJECT)
        return request(NODE_IP[node], DEV.http_port(self.base, node), 'GET',
                       f'/block/billing/{volume}?offset={offset}&len={length}',
                       headers=headers, timeout=timeout)


def install_init():
    """Keep the guest init in the image identical to the one in this repository.

    The init is part of the fixture, not of the store, so it is copied in
    rather than edited in place; a stale copy would silently change what the
    VMs boot.
    """
    source = REPO / 'scripts' / 'microvm-init.sh'
    target = STORE / 'root' / 'init'
    if target.is_file() and target.read_bytes() == source.read_bytes():
        return
    target.write_bytes(source.read_bytes())
    target.chmod(0o755)
    print(f'guest init installed: {target}')


def build_rootfs(force=False):
    """Lay the prepared tree down as the shared read-only rootfs image."""
    install_init()
    inputs = [STORE / 'root' / 'init', STORE / 'root' / 'usr' / 'local' / 'bin' / 'yadbd']
    if ROOTFS.exists() and not force and all(
            ROOTFS.stat().st_mtime > p.stat().st_mtime for p in inputs):
        return
    for path in inputs:
        if not path.exists():
            raise RuntimeError(f'{path} missing; see scripts/README-microvm.md')
    if ROOTFS.exists():
        ROOTFS.unlink()
    with open(ROOTFS, 'wb') as handle:
        handle.truncate(ROOTFS_MIB * 1024 * 1024)
    run('mkfs.ext4', '-q', '-F', '-d', str(STORE / 'root'), '-L', 'yadbroot', str(ROOTFS))
    print(f'rootfs rebuilt: {ROOTFS}')


def scenario_up(cluster):
    """Bring the cluster up and prove every group is led and applied."""
    cluster.wait_leaders()
    leaders = {group: cluster.leader_of(1, group) for group in DEV.GROUPS}
    print(f'cluster up: leaders {leaders}', flush=True)
    cluster.apply(1, DEV.PLATFORM.read_text())
    cluster.wait_applied()
    print('platform applied; every replica applied its complete log', flush=True)

    # The deployment-facing readiness claim must be the structured one, and it
    # must arrive over the guest's own listener rather than a host shortcut.
    health = json.loads(request(NODE_IP[1], DEV.http_port(cluster.base, 1), 'GET',
                                '/healthz', headers=DEV.signed_headers(
                                    cluster.secret, DEV.ADMIN))[2])
    assert health['readiness']['ready'] is True, health['readiness']
    assert health['readiness']['limits'], health['readiness']
    status = cluster.status(1)
    assert status['readiness']['ready'] is True, status['readiness']
    print(f'readiness reported by the guest: ready={health["readiness"]["ready"]}, '
          f'{len(health["readiness"]["limits"])} named limits', flush=True)


def scenario_services(cluster):
    """One write per service through a shard *follower*, then a direct one.

    A shard is one Raft group with one leader, and only that leader commits.
    The deployment's clients may address any node, so a follower that answers
    `200` can only have done so by forwarding to the shard's leader: a follower
    cannot commit on its own. That is the property checked here, and it is why
    the entry node is chosen as a follower of the project's group rather than
    as "some node that is not the control leader".
    """
    leaders = {group: cluster.leader_of(1, group) for group in DEV.GROUPS}
    control_group = DEV.CONTROL_GROUP
    # `billing` is the single project in examples/platform.yaml; the apply
    # assigns it project id 1, which the manifest maps to its data group. The
    # mapping is asserted below from the log itself instead of trusted.
    project_group = DEV.PROJECT_GROUPS['1']
    shard_leader = leaders[project_group]
    entry = next(node for node in NODES if node != shard_leader)
    entry_state = cluster.group_row(entry, project_group)['state']
    assert entry_state != 'Leader', (
        f'entry node {entry} leads group {project_group}; the forwarding path '
        f'would not be exercised')
    print(f'topology: control group {control_group} led by {leaders[control_group]}; '
          f'project group {project_group} led by {shard_leader}; '
          f'writing through node {entry} ({entry_state} of group {project_group})',
          flush=True)

    before = cluster.applied_map(1)

    status, body = cluster.sql(entry, 'CREATE TABLE IF NOT EXISTS gate (id INT PRIMARY KEY, note TEXT)')
    assert status in (200, 201), f'CREATE TABLE HTTP {status}: {body[:200]!r}'
    status, body = cluster.sql(
        entry, f"INSERT INTO gate (id, note) VALUES (1, '{GATE_NOTE}')")
    assert status in (200, 201), f'INSERT HTTP {status}: {body[:200]!r}'
    print(f'SQL write acknowledged by a follower of group {project_group}', flush=True)

    # Which group actually took the entry? The write is a SQL statement, so the
    # project's data group must advance and the answer comes from the log, not
    # from the harness's own manifest.
    def committed_in_project_group():
        now = cluster.applied_map(1)
        advanced = {group for group, index in now.items()
                    if before.get(group) is not None and index > before[group]}
        if not advanced:
            return 'no group advanced yet'
        if advanced != {project_group}:
            return f'advanced {sorted(advanced)}, expected only {project_group}'
        return None

    eventually('the SQL write advances exactly the project group',
               committed_in_project_group, 60)
    print(f'the write committed in group {project_group}, not in the control group',
          flush=True)

    payload = BLOCK_PAYLOAD
    status, _, body = cluster.block_write(entry, 'vm-root', 0, payload)
    assert status in (200, 201, 204), f'block PUT HTTP {status}: {body[:200]!r}'
    print(f'block write acknowledged by a follower of group {project_group}', flush=True)

    # The database must answer over the real PostgreSQL wire, not only through
    # the HTTP query route.
    conn = cluster.pg(entry)
    try:
        conn.query('CREATE TABLE IF NOT EXISTS pg_gate (id INT PRIMARY KEY, note TEXT)')
        conn.query(f"INSERT INTO pg_gate (id, note) VALUES (1, '{PG_NOTE}')")
        assert conn.rows('SELECT note FROM pg_gate WHERE id = 1') == [[PG_NOTE]]
    finally:
        conn.close()
    print(f'pgwire write and read acknowledged through node {entry}', flush=True)

    object_body = S3_BODY
    status, _, body = cluster.s3_put(entry, S3_KEY, object_body)
    assert status in (200, 201, 204), f'S3 PUT HTTP {status}: {body[:200]!r}'
    print(f'S3 write acknowledged by a follower of group {project_group}', flush=True)

    # The direct path: a write addressed to the shard's own leader commits
    # without any forwarding, which is the ordinary YDB-shaped path.
    status, body = cluster.sql(
        shard_leader, "INSERT INTO gate (id, note) VALUES (2, 'direct-leader')")
    assert status in (200, 201), f'INSERT on the shard leader HTTP {status}: {body[:200]!r}'
    print(f'SQL write acknowledged directly by the group {project_group} leader '
          f'{shard_leader}', flush=True)

    # Read back through every node, which waits out ordinary follower lag.
    def all_read():
        for node in NODES:
            s, b = cluster.sql(node, 'SELECT note FROM gate WHERE id = 1')
            if s != 200 or GATE_NOTE.encode() not in b:
                return f'SQL on node {node}: HTTP {s} {b[:120]!r}'
            s, b = cluster.sql(node, 'SELECT note FROM gate WHERE id = 2')
            if s != 200 or b'direct-leader' not in b:
                return f'SQL (direct) on node {node}: HTTP {s} {b[:120]!r}'
            s, _, b = cluster.block_read(node, 'vm-root', 0, len(payload))
            if s != 200 or b != payload:
                return f'block on node {node}: HTTP {s} {b[:120]!r}'
            s, _, b = cluster.s3_get(node, S3_KEY)
            if s != 200 or b != object_body:
                return f'S3 on node {node}: HTTP {s} {b[:120]!r}'
            try:
                conn = cluster.pg(node)
                try:
                    got = conn.rows('SELECT note FROM pg_gate WHERE id = 1')
                finally:
                    conn.close()
            except (OSError, RuntimeError) as error:
                return f'pgwire on node {node}: {type(error).__name__}: {error}'
            if got != [[PG_NOTE]]:
                return f'pgwire on node {node}: {got!r}'
        return None
    eventually('SQL, block, S3 and pgwire read back on all three nodes', all_read, 120)
    print('SQL, block, S3 and pgwire read back byte-exactly on all three nodes', flush=True)

    rows = cluster.group_rows(1)
    assert set(rows) == set(DEV.GROUPS), f'node 1 reports groups {sorted(rows)}'
    print(f'groups observed: {sorted(rows)}; project groups '
          f'{DEV.PROJECT_GROUPS}', flush=True)


def scenario_power_cut(cluster):
    """Kill every VMM at once, then prove the acknowledged writes are on disk.

    This is the strongest claim this fixture can make. With all three guests
    dead there is no peer to copy anything from, so whatever a node serves
    after the restart came off its own backing file. The writes being checked
    were acknowledged before the cut and are read back through each node's own
    listener with `force_leader: false`, i.e. from that node's state machine.
    """
    before = {node: {row['group']: applied_index(row) for row in cluster.local_groups(node)}
              for node in NODES}
    for node in NODES:
        cluster.kill_vm(node)
    print('all three VMMs killed: cluster-wide power cut', flush=True)

    for node in NODES:
        cluster.prepare_restart(node)
        cluster.start_vm(node)
    cluster.wait_leaders(240)
    cluster.wait_applied(240)
    print('every group re-elected a leader and applied its complete log', flush=True)

    def local_reads():
        for node in NODES:
            s, b = cluster.sql(node, 'SELECT note FROM gate WHERE id = 1')
            if s != 200 or GATE_NOTE.encode() not in b:
                return f'SQL on node {node}: HTTP {s} {b[:120]!r}'
            s, _, b = cluster.block_read(node, 'vm-root', 0, len(BLOCK_PAYLOAD))
            if s != 200 or b != BLOCK_PAYLOAD:
                return f'block on node {node}: HTTP {s} {b[:120]!r}'
            s, _, b = cluster.s3_get(node, S3_KEY)
            if s != 200 or b != S3_BODY:
                return f'S3 on node {node}: HTTP {s} {b[:120]!r}'
            try:
                conn = cluster.pg(node)
                try:
                    got = conn.rows('SELECT note FROM pg_gate WHERE id = 1')
                finally:
                    conn.close()
            except (OSError, RuntimeError) as error:
                return f'pgwire on node {node}: {type(error).__name__}: {error}'
            if got != [[PG_NOTE]]:
                return f'pgwire on node {node}: {got!r}'
        return None
    eventually('the pre-cut writes are served locally on all three nodes', local_reads, 180)
    print('the pre-cut SQL, block, S3 and pgwire writes are served locally '
          'on all three nodes', flush=True)

    # A committed entry cannot disappear from a node's log: if the applied
    # index went backwards, the disk lost data that had been acknowledged.
    for node in NODES:
        for row in cluster.local_groups(node):
            was = before[node].get(row['group'])
            now = applied_index(row)
            assert now is not None and was is not None and now >= was, (
                f'node {node} group {row["group"]}: applied index went from {was} to {now}')
    print('every node still holds every pre-cut entry it had applied', flush=True)


def scenario_lose_one(cluster):
    """Kill one VMM and prove the survivors keep acknowledging writes."""
    before = cluster.local_groups(1)
    victim = max(NODES, key=lambda n: sum(
        1 for row in cluster.local_groups(n) if row['leader_id'] == n))
    survivors = [n for n in NODES if n != victim]
    print(f'losing node {victim} (leads {sum(1 for r in before if r["leader_id"] == victim)} groups); '
          f'survivors {survivors}', flush=True)
    cluster.kill_vm(victim)

    # Survivors must elect replacements and keep serving.
    def survivors_lead():
        for node in survivors:
            for row in cluster.local_groups(node):
                if row['leader_id'] is None:
                    return False
        return True
    wait_for('the survivors elect leaders for every group', survivors_lead, 180)
    print('survivors elected leaders for every group', flush=True)

    writer = survivors[0]
    status, body = cluster.sql(writer, "INSERT INTO gate (id, note) VALUES (3, 'after-loss')")
    assert status in (200, 201), f'INSERT after loss HTTP {status}: {body[:200]!r}'
    print(f'SQL write acknowledged on {writer} with node {victim} down', flush=True)

    payload = b'block-after-loss'
    status, _, body = cluster.block_write(writer, 'vm-root', 64, payload)
    assert status in (200, 201, 204), f'block PUT after loss HTTP {status}: {body[:200]!r}'
    object_body = b's3-after-loss'
    status, _, body = cluster.s3_put(writer, '/invoices-2026/gate-after-loss', object_body)
    assert status in (200, 201, 204), f'S3 PUT after loss HTTP {status}: {body[:200]!r}'
    print(f'block and S3 writes acknowledged on {writer} with node {victim} down', flush=True)

    def survivors_read():
        for node in survivors:
            s, b = cluster.sql(node, 'SELECT note FROM gate WHERE id = 3')
            if s != 200 or b'after-loss' not in b:
                return f'SQL on node {node}: HTTP {s} {b[:120]!r}'
            s, _, b = cluster.block_read(node, 'vm-root', 64, len(payload))
            if s != 200 or b != payload:
                return f'block on node {node}: HTTP {s} {b[:120]!r}'
            s, _, b = cluster.s3_get(node, '/invoices-2026/gate-after-loss')
            if s != 200 or b != object_body:
                return f'S3 on node {node}: HTTP {s} {b[:120]!r}'
        return None
    eventually('the survivors read the post-loss writes', survivors_read, 120)
    print('survivors read every post-loss write', flush=True)

    # The victim is gone, so it cannot have applied them: that is the point.
    return victim, survivors


def scenario_recover(cluster, victim, survivors):
    """Restart the killed VMM from its own disk and prove it catches up."""
    cluster.prepare_restart(victim)
    cluster.start_vm(victim)
    wait_for(f'node {victim} answers /openraft/status again',
             lambda: bool(cluster.status(victim)), 180)

    def caught_up():
        for row in cluster.local_groups(victim):
            if applied_index(row) != row['last_log_index']:
                return False
        return True
    wait_for(f'node {victim} applied its complete log again', caught_up, 240)

    def reads_everything():
        s, b = cluster.sql(victim, 'SELECT note FROM gate WHERE id = 3')
        return s == 200 and b'after-loss' in b
    wait_for(f'node {victim} reads the writes made while it was down', reads_everything, 180)
    print(f'node {victim} restarted from its own disk and caught up', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--port-base', type=int, default=23600)
    parser.add_argument('--keep', action='store_true', help='keep the run directory')
    parser.add_argument('--rebuild-rootfs', action='store_true')
    parser.add_argument('--only', choices=['up', 'services', 'powercut', 'lose', 'all'], default='all')
    args = parser.parse_args()
    DEV.validate_base(args.port_base)
    if not KERNEL.is_file():
        raise RuntimeError(f'kernel missing: {KERNEL}')

    if os.geteuid() != 0:
        # Re-exec once as root. Keeping the harness in one process means a
        # recorded PID is the VMM's own PID, so kill -9 is a real power cut
        # rather than a signal to a sudo wrapper.
        env = dict(os.environ)
        # `sudo -E` is refused by this host's sudoers, and nothing in the
        # environment is needed: the run directory and the store are fixed.
        os.execvpe('sudo', ['sudo', '-n', sys.executable, str(Path(__file__).resolve()),
                            *sys.argv[1:]], env)

    build_rootfs(force=args.rebuild_rootfs)

    started = time.monotonic()
    root = Path(tempfile.mkdtemp(
        prefix='yadb-microvm-', dir=os.environ.get('YADB_TEST_TMPDIR', '/tmp/opencode')))
    cluster = Cluster(root, args.port_base, keep=args.keep)
    victim = None
    try:
        (root / 'provision').mkdir(parents=True, exist_ok=True)
        DEV.provision(root / 'provision', args.port_base)
        secrets = DEV.load_secrets(root / 'provision')
        cluster.secret = secrets['subject']
        cluster.protocol_secret = secrets['raft']
        cluster.user_secret = DEV.ensure_credentials(root / 'provision')
        cluster.link_up()
        for node in NODES:
            cluster.build_node_image(node)
            cluster.write_vm_config(node)
            cluster.start_vm(node)
        scenario_up(cluster)
        if args.only in ('services', 'all'):
            scenario_services(cluster)
        if args.only in ('powercut', 'all'):
            scenario_power_cut(cluster)
        if args.only in ('lose', 'all'):
            victim, survivors = scenario_lose_one(cluster)
            scenario_recover(cluster, victim, survivors)
            victim = None
    finally:
        for node in list(cluster.procs):
            cluster.stop_vm(node)
        cluster.link_down()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f'kept {root}')
    print(f'finished in {time.monotonic() - started:.1f}s')


if __name__ == '__main__':
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except (RuntimeError, AssertionError, subprocess.SubprocessError, KeyboardInterrupt) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        sys.exit(1)
