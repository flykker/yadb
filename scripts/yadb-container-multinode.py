#!/usr/bin/env python3
"""Exercise three host-built OpenRaft daemons on a disposable Podman bridge.

Requires rootless Podman, ubuntu:26.04, iproute2 on the host, Python cryptography,
and a host-built target/debug/yadbd with openraft-storage. No image is built.
This is one kernel, not a multi-host, power-loss, or RPC-reordering test.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

REPO = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('yadb_dev', REPO / 'scripts/yadb-openraft-dev.py')
DEV = importlib.util.module_from_spec(spec)
spec.loader.exec_module(DEV)
NODES = (1, 2, 3)


def run(*args, check=True, timeout=30):
    return subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          check=check, timeout=timeout)


def wait_for(description, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, ValueError, KeyError, urllib.error.URLError):
            pass
        time.sleep(.25)
    raise AssertionError(f'{description}: timed out; last={last!r}')


class Cluster:
    def __init__(self, root, base, image):
        self.root, self.base, self.image = root, base, image
        self.label = 'yadb-multinode-' + uuid.uuid4().hex[:12]
        self.network = self.label
        self.names = {n: f'{self.label}-{n}' for n in NODES}
        self.ips = {}
        self.secret = None

    def podman(self, *args, **kwargs):
        return run('podman', *args, **kwargs)

    def exec(self, node, *args, check=True):
        return self.podman('exec', self.names[node], *args, check=check)

    def setup(self):
        self.podman('network', 'create', '--label', f'yadb.test={self.label}', self.network)
        DEV.provision(self.root, self.base)
        self.secret = DEV.load_secrets(self.root)['subject']
        # The sleeping container anchors its namespace. Daemons are launched only
        # after all IP addresses and configs are known.
        for n in NODES:
            self.podman('run', '-d', '--name', self.names[n], '--label', f'yadb.test={self.label}',
                        '--network', self.network, '--cap-add=NET_ADMIN',
                        '-p', f'127.0.0.1:{DEV.http_port(self.base, n)}:{DEV.http_port(self.base, n)}',
                        '-v', f'{REPO}:{REPO}:ro', '-v', f'{self.root}:{self.root}:rw',
                        '-v', '/usr/sbin/tc:/usr/sbin/tc:ro',
                        '-v', '/usr/lib/x86_64-linux-gnu:/hostlib:ro',
                        '-v', '/usr/lib/x86_64-linux-gnu/tc:/usr/lib/x86_64-linux-gnu/tc:ro',
                        '-e', 'LD_LIBRARY_PATH=/hostlib', self.image, 'sleep', 'infinity')
            info = json.loads(self.podman('inspect', self.names[n]).stdout)[0]
            self.ips[n] = info['NetworkSettings']['Networks'][self.network]['IPAddress']
        ns = {n: self.exec(n, 'readlink', '/proc/self/ns/net').stdout.strip() for n in NODES}
        host_ns = os.readlink('/proc/self/ns/net')
        assert len(set(self.ips.values())) == 3 and len(set(ns.values())) == 3 and host_ns not in ns.values(), f'containers do not have distinct IPs and network namespaces: ips={self.ips} namespaces={ns} host={host_ns}'
        print(f'topology: ips={self.ips} namespaces={ns}', flush=True)
        for n in NODES:
            (self.root / f'metadata-{n}').mkdir(mode=0o700)
            for group in NODES:
                path = self.root / 'config' / f'node-{n}-group-{group}.json'
                cfg = json.loads(path.read_text())
                for peer in cfg['peers']:
                    peer['address'] = f"{self.ips[peer['id']]}:{DEV.raft_port(self.base, group, peer['id'])}"
                cfg['listen'] = f'0.0.0.0:{DEV.raft_port(self.base, group, n)}'
                # Separate metadata roots for each container, in addition to data dirs.
                cfg['root'] = str(self.root / f'metadata-{n}')
                DEV.LOCAL.replace(path, cfg)
            command = DEV.node_command(self.root, self.base, n)
            for i, arg in enumerate(command):
                if arg.startswith('127.0.0.1:'):
                    command[i] = arg.replace('127.0.0.1:', '0.0.0.0:', 1)
            command[command.index('--listen') + 1] = f'{self.ips[n]}:{DEV.http_port(self.base, n)}'
            idx = command.index('--peers') + 1
            command[idx] = ','.join(f'{i}={self.ips[i]}:{DEV.http_port(self.base, i)}' for i in NODES)
            env = DEV.daemon_env(self.root)
            self.podman('exec', '-d', '-e', f'YADB_SUBJECT_SECRET={env["YADB_SUBJECT_SECRET"]}',
                        '-e', f'YADB_RAFT_TOKEN={env["YADB_RAFT_TOKEN"]}', self.names[n], 'sh', '-c',
                        f'exec {shlex.join(command)} >{shlex.quote(str(self.root / "logs" / f"node-{n}.log"))} 2>&1')
        try:
            wait_for('all three group leaders', lambda: all(
                len(self.status(n)) == 3 and all(g['leader_id'] is not None for g in self.status(n)) for n in NODES), 30)
        except AssertionError:
            for n in NODES:
                print(f'node {n} log: {(self.root / "logs" / f"node-{n}.log").read_text(errors="replace")}', file=sys.stderr)
            raise

    def request(self, node, path, body=None, timeout=10):
        headers = DEV.signed_headers(self.secret, DEV.ADMIN)
        if body is not None:
            headers['Content-Type'] = 'application/json'
            body = json.dumps(body).encode()
        request = urllib.request.Request(f'http://127.0.0.1:{DEV.http_port(self.base, node)}{path}', data=body, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError(f'node {node} {path}: HTTP {error.code}: {error.read().decode(errors="replace")}') from error

    def status(self, node):
        return self.request(node, '/openraft/status')['local_observations']

    def leader(self, node=1, group=1):
        return next(g['leader_id'] for g in self.status(node) if g['group'] == group)

    def apply(self, node, text):
        return self.request(node, '/v1/apply', {'yaml': text}, timeout=60)

    def qdisc(self, node, *args):
        return self.exec(node, 'tc', 'qdisc', *args, 'dev', 'eth0')

    def show_qdisc(self, node):
        return self.qdisc(node, 'show').stdout

    def container_status(self, node):
        # Query the node's own public listener from inside that container.
        script = ('exec 3<>/dev/tcp/$1/$2; '
                  'printf "GET /v1/status HTTP/1.0\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n" >&3; '
                  'dd bs=1 count=100000 <&3 2>/dev/null || true')
        result = self.exec(node, 'bash', '-c', script, 'status', self.ips[node], str(DEV.http_port(self.base, node)), check=False)
        assert result.returncode == 0, f'container {node} local status failed: {result.stderr}'
        raw = result.stdout.replace('\r\n', '\n')
        head, body = raw.split('\n\n', 1)
        assert re.match(r'HTTP/1\.[01] 200', head), f'container status failed: {head}'
        return json.loads(body)

    def converge(self):
        def caught_up():
            indexes = {group: set() for group in NODES}
            for n in NODES:
                for group in self.status(n):
                    match = re.search(r'index: (\d+)', group['last_applied'])
                    if not match or int(match.group(1)) != group['last_log_index']:
                        return False
                    indexes[group['group']].add(group['last_log_index'])
            return all(len(values) == 1 for values in indexes.values())
        wait_for('replicas applied their complete logs', caught_up)

    def read_everywhere(self, expected):
        for node in NODES:
            wait_for(f'node {node} observes {expected}', lambda: all(
                item in self.container_status(node)['pools'] for item in expected))

    def create_pool(self, node, name):
        yaml = f'''apiVersion: yadb.io/v1alpha1
kind: StoragePool
metadata:
  name: {name}
spec:
  erasure:
    scheme: mirror
    replicas: 3
  failureDomains: 3
'''
        result = self.apply(node, yaml)
        assert result.get('generation'), f'write was not acknowledged: {result}'
        print(f'acknowledged {name}: {result}', flush=True)

    def reachable(self, source, destination):
        cmd = 'timeout 3 bash -c \'exec 3<>/dev/tcp/$1/$2\' probe "$1" "$2"'
        return self.exec(source, 'bash', '-c', cmd, 'probe', self.ips[destination],
                         str(DEV.raft_port(self.base, 1, destination)), check=False).returncode == 0

    def cleanup(self):
        errors = []
        for name in self.names.values():
            result = self.podman('rm', '-f', '-t', '1', name, check=False, timeout=30)
            if result.returncode and 'no container with name' not in result.stderr.lower():
                errors.append(f'{name}: {result.stderr.strip()}')
        if self.podman('network', 'exists', self.network, check=False).returncode == 0:
            result = self.podman('network', 'rm', self.network, check=False)
            if result.returncode:
                errors.append(f'network {self.network}: {result.stderr.strip()}')
        if self.podman('network', 'exists', self.network, check=False).returncode == 0:
            errors.append(f'network remains: {self.network}')
        remaining = self.podman('ps', '-a', '--filter', f'label=yadb.test={self.label}', '--format', '{{.Names}}').stdout.strip()
        if remaining:
            errors.append(f'containers remain: {remaining}')
        if errors:
            raise RuntimeError('cleanup failed: ' + '; '.join(errors))
        print('cleanup: no test containers or network remain', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port-base', type=int, default=24600)
    parser.add_argument('--image', default='ubuntu:26.04')
    args = parser.parse_args()
    DEV.validate_base(args.port_base)
    if not DEV.DAEMON.is_file():
        raise RuntimeError(f'build the host daemon first: {DEV.DAEMON}')
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='yadb-containers-', dir=os.environ.get('YADB_TEST_TMPDIR', '/tmp/opencode')) as directory:
        cluster = Cluster(Path(directory), args.port_base, args.image)
        try:
            cluster.setup()
            cluster.apply(1, DEV.PLATFORM.read_text())
            cluster.create_pool(1, 'container-before')
            leader = cluster.leader()
            follower = next(n for n in NODES if n != leader)
            wait_for('nonleader container reads acknowledged project',
                     lambda: 'container-before' in cluster.container_status(follower)['pools'])
            print(f'cross-container commit read on follower {follower}, leader {leader}', flush=True)

            # Isolate the current control-group leader in its egress direction.
            isolated = leader
            survivors = [n for n in NODES if n != isolated]
            for n in survivors:
                assert cluster.reachable(n, isolated), f'baseline node {n} cannot reach {isolated}'
            cluster.exec(isolated, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem', 'loss', '100%')
            qdisc = cluster.show_qdisc(isolated)
            assert 'netem' in qdisc and 'loss 100%' in qdisc, f'partition qdisc absent: {qdisc}'
            for n in survivors:
                assert not cluster.reachable(n, isolated), f'partition failed: node {n} can still reach isolated node {isolated}'
            new_leader = wait_for('survivors elect a new leader', lambda: (
                (lambda a, b: a if a == b and a in survivors else None)(cluster.leader(survivors[0]), cluster.leader(survivors[1]))))
            assert new_leader != isolated
            cluster.create_pool(new_leader, 'container-partition')
            cluster.exec(isolated, 'tc', 'qdisc', 'del', 'dev', 'eth0', 'root')
            cluster.read_everywhere(['container-before', 'container-partition'])
            cluster.converge()
            print('partition healed and all replicas caught up', flush=True)

            for n in NODES:
                cluster.exec(n, 'tc', 'qdisc', 'add', 'dev', 'eth0', 'root', 'netem',
                             'delay', '20ms', '10ms', 'distribution', 'normal', 'loss', '10%')
            for n in NODES:
                qdisc = cluster.show_qdisc(n)
                assert re.search(r'netem .*delay 20ms\s+10ms loss 10%', qdisc), f'node {n} missing expected delay/loss netem: {qdisc}'
            cluster.create_pool(cluster.leader(), 'container-loss')
            cluster.read_everywhere(['container-before', 'container-partition', 'container-loss'])
            cluster.converge()
            for n in NODES:
                cluster.exec(n, 'tc', 'qdisc', 'del', 'dev', 'eth0', 'root')
                assert 'netem' not in cluster.show_qdisc(n), f'node {n} netem not removed'
            cluster.create_pool(cluster.leader(), 'container-healed')
            cluster.read_everywhere(['container-before', 'container-partition', 'container-loss', 'container-healed'])
            cluster.converge()
            print('loss/delay and clean recovery passed', flush=True)
        finally:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            cluster.cleanup()
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
