#!/usr/bin/env python3
"""One-shot local writes and bounded, read-only live/reopen verification."""

import argparse
import base64
import hashlib
import hmac
import http.client
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import uuid

spec = importlib.util.spec_from_file_location('yadb_local', Path(__file__).with_name('yadb-local.py'))
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)

SUBJECT = 'local-operator'
PROJECTS = {'alpha': (1, 2), 'beta': (2, 3)}
YAML = '''apiVersion: yadb.io/v1alpha1
kind: StoragePool
metadata: {name: data}
spec: {erasure: {scheme: mirror, replicas: 3}, failureDomains: 3}
'''
for name in PROJECTS:
    YAML += f'''---
apiVersion: yadb.io/v1alpha1
kind: Project
metadata: {{name: {name}}}
spec: {{quotas: {{maxStorageBytes: 100000000, maxIops: 1000}}, members: [{{subject: local-operator, role: readWrite}}]}}
'''
for name in PROJECTS:
    YAML += f'''---
apiVersion: yadb.io/v1alpha1
kind: Database
metadata: {{name: db, project: {name}}}
spec: {{walPool: data, dataPool: data, isolation: shared, shards: 2}}
---
apiVersion: yadb.io/v1alpha1
kind: ObjectStore
metadata: {{name: store, project: {name}}}
spec: {{pool: data, isolation: shared, buckets: [bucket]}}
---
apiVersion: yadb.io/v1alpha1
kind: BlockVolume
metadata: {{name: disk, project: {name}}}
spec: {{pool: data, sizeBytes: 8192}}
'''


def payload(value):
    return json.dumps(value).encode()


def deployment_http_base(root):
    return local.deployment_ports(root)


def pattern(project):
    return (project.encode() * 512)[:512]


class Smoke:
    def __init__(self, root, command):
        self.http_base = deployment_http_base(root)
        secret = json.loads((root / 'credentials/secrets.json').read_text())['subject']
        self.signature = hmac.new(secret.encode(), SUBJECT.encode(), hashlib.sha256).hexdigest()
        self.path = root / 'logs' / f'smoke-{command}-{uuid.uuid4().hex}.json'
        self.evidence = {'command': command, 'project_group_map': PROJECTS, 'requests': []}
        # Create exclusively before making any request; never overwrite prior evidence.
        with self.path.open('x') as f:
            json.dump(self.evidence, f)

    def save(self):
        with self.path.open('w') as f:
            json.dump(self.evidence, f, indent=2)

    def request(self, node, method, path, body=b'', content_type='application/json'):
        entry = {'node': node, 'method': method, 'path': path, 'outcome': 'started'}
        self.evidence['requests'].append(entry)
        self.save()
        conn = http.client.HTTPConnection('127.0.0.1', self.http_base + node - 1, timeout=5)
        try:
            conn.request(method, path, body, {'Content-Type': content_type,
                         'x-yadb-subject': SUBJECT, 'x-yadb-subject-signature': self.signature})
            response = conn.getresponse()
            entry['status'] = response.status
            entry['headers'] = [[k, v] for k, v in response.getheaders()
                                if k.lower() not in ('set-cookie', 'authorization', 'x-yadb-subject-signature')]
            self.save()
            data = response.read()
            entry['outcome'] = 'response'
            entry['body_base64'] = base64.b64encode(data).decode()
            self.save()
            return response.status, data
        except (OSError, http.client.HTTPException) as exc:
            entry['outcome'] = 'transport error; mutation outcome may be ambiguous'
            entry['error'] = type(exc).__name__
            self.save()
            raise RuntimeError(f'{method} {path} node {node}: transport error; do not retry setup') from exc
        finally:
            conn.close()

    def mutate(self, method, path, body, content_type='application/json'):
        status, _ = self.request(1, method, path, body, content_type)
        if not 200 <= status < 300:
            raise RuntimeError(f'{method} {path} returned {status}; do not retry setup')

    def poll(self, label, check, timeout=30):
        deadline = time.monotonic() + timeout
        while True:
            try:
                if check():
                    return
            except (RuntimeError, ValueError, KeyError, TypeError):
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError(f'{label} did not converge in the nominal {timeout}s polling window; inspect evidence')
            time.sleep(0.5)

    def control_ready(self):
        for node in range(1, 4):
            status, data = self.request(node, 'GET', '/v1/status')
            if status != 200:
                return False
            projects = json.loads(data)['projects']
            for name, (pid, _) in PROJECTS.items():
                if name not in projects:
                    return False
                # ControlPlane stores each project as [id, spec].
                if projects[name][0] != pid:
                    raise RuntimeError(f'Unexpected project ID for {name}')
        return True

    def statuses(self):
        for node in range(1, 4):
            status, data = self.request(node, 'GET', '/openraft/status')
            if status != 200:
                raise RuntimeError(f'node {node} group status returned {status}')
            observations = json.loads(data)['local_observations']
            if not {1, 2, 3}.issubset({item['group'] for item in observations}):
                raise RuntimeError(f'node {node} missing group status')

    def readback(self):
        for node in range(1, 4):
            for project in PROJECTS:
                status, data = self.request(node, 'POST', f'/db/{project}/db/query',
                    payload({'sql': 'SELECT v FROM local_smoke WHERE id = 1'}))
                if status != 200:
                    return False
                result = json.loads(data)
                if result['columns'] != ['v'] or result['rows'] != [[project]]:
                    return False
                status, data = self.request(node, 'GET', f'/s3/{project}/store/bucket/local-smoke')
                if status != 200 or data != project.encode():
                    return False
                status, data = self.request(node, 'GET', f'/block/{project}/disk?offset=0&len=512')
                if status != 200 or data != pattern(project):
                    return False
        return True

    def setup(self, root):
        marker = root / 'logs' / 'smoke-setup-started'
        with marker.open('x') as f:
            f.write(f'evidence: {self.path.name}\nNever automatically retry setup.\n')
            f.flush()
            os.fsync(f.fileno())
        self.mutate('POST', '/v1/apply', payload({'yaml': YAML}))
        self.poll('control cache', self.control_ready)
        self.statuses()
        for project in PROJECTS:
            path = f'/db/{project}/db/query'
            for sql in ('CREATE TABLE local_smoke (id INT PRIMARY KEY, v TEXT)',
                        f"INSERT INTO local_smoke VALUES (1,'{project}')"):
                self.mutate('POST', path, payload({'sql': sql}))
            self.mutate('PUT', f'/s3/{project}/store/bucket/local-smoke', project.encode(), 'application/octet-stream')
            self.mutate('PUT', f'/block/{project}/disk?offset=0', pattern(project), 'application/octet-stream')

    def check(self):
        self.poll('control cache', self.control_ready)
        self.statuses()
        self.poll('readback', self.readback)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('setup', 'check'))
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    smoke = Smoke(root, args.command)
    print(f'Evidence: {smoke.path}', flush=True)
    try:
        if args.command == 'setup':
            smoke.setup(root)
        else:
            smoke.check()
        smoke.evidence['result'] = 'success'
        print('Success; readback through all three HTTP endpoints does not prove independent replica-local application or durable replication.' if args.command == 'check' else 'Setup complete; run check separately.')
    except Exception as exc:
        smoke.evidence['result'] = 'failed'
        smoke.evidence['error'] = str(exc)
        raise
    finally:
        smoke.save()


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print(f'Error: {error}', file=sys.stderr)
        sys.exit(1)
