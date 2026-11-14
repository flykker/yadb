import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('yadb_local_smoke', Path(__file__).with_name('yadb-local-smoke.py'))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class PortBaseTest(unittest.TestCase):
    def make_identity(self, root, base):
        config = root / 'config'
        config.mkdir()
        (config / 'ports.json').write_text(json.dumps({'http_base': base}))
        for node in range(1, 4):
            for group in range(1, 4):
                (config / f'node-{node}-group-{group}.json').write_text(json.dumps({
                    'node': node, 'group': group, 'listen': f'127.0.0.1:{base + 10000 + (group - 1) * 3 + node - 1}',
                    'peers': []}))
        (config / 'identity.json').write_text(json.dumps({'http_base': base, 'node_ids': [1, 2, 3],
            'deployment_id': 'test', 'listener_sha256': smoke.local.listener_hash(root, base)}))

    def test_legacy_root_without_identity_falls_back_to_default_base(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(smoke.deployment_http_base(Path(directory)), 8420)

    def test_recorded_base_is_used(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_identity(root, 9420)
            self.assertEqual(smoke.deployment_http_base(root), 9420)

    def test_requests_follow_the_recorded_base(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'credentials').mkdir()
            (root / 'logs').mkdir()
            (root / 'credentials' / 'secrets.json').write_text(json.dumps({'subject': 'secret'}))
            self.make_identity(root, 9420)
            client = smoke.Smoke(root, 'check')
            with mock.patch.object(smoke.http.client, 'HTTPConnection') as connection:
                response = connection.return_value.getresponse.return_value
                response.status = 200
                response.read.return_value = b'{}'
                response.getheaders.return_value = [('content-type', 'application/json')]
                client.request(3, 'GET', '/healthz')
            connection.assert_called_once_with('127.0.0.1', 9422, timeout=5)

    def test_invalid_identity_prevents_marker_and_requests_for_both_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'config').mkdir()
            (root / 'logs').mkdir()
            (root / 'config' / 'ports.json').write_text('{"http_base": 9420}')
            with mock.patch.object(smoke.http.client, 'HTTPConnection') as connection:
                for command in ('setup', 'check'):
                    with self.subTest(command=command), self.assertRaisesRegex(ValueError, 'identity.json is missing'):
                        smoke.Smoke(root, command)
                connection.assert_not_called()
            self.assertFalse((root / 'logs' / 'smoke-setup-started').exists())
            self.assertEqual(list((root / 'logs').iterdir()), [])


class OperatorIdentityTest(unittest.TestCase):
    """run provisions the allowlist; smoke signs with it. They must agree."""

    def test_smoke_subject_is_the_provisioned_operator(self):
        self.assertEqual(smoke.SUBJECT, smoke.local.OPERATOR)
        self.assertTrue(smoke.SUBJECT)


if __name__ == '__main__':
    unittest.main()
