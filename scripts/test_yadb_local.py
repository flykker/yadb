import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location('yadb_local', Path(__file__).with_name('yadb-local.py'))
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)


class FakeChild:
    def __init__(self, pid, signal_error=False, wait_error=False):
        self.pid = pid
        self.signal_error = signal_error
        self.wait_error = wait_error
        self.exit_code = None
        self.signalled = False
        self.waited = False

    def poll(self):
        return self.exit_code

    def send_signal(self, sig):
        self.signalled = True
        if self.signal_error:
            raise ProcessLookupError('simulated signal race')
        self.exit_code = 0

    def wait(self, timeout):
        self.waited = True
        if self.wait_error:
            raise OSError('simulated wait error')
        if self.exit_code is None:
            raise subprocess.TimeoutExpired('fake', timeout)
        return self.exit_code


class CleanupTest(unittest.TestCase):
    def test_signal_race_does_not_skip_other_children_or_state(self):
        children = [FakeChild(1, signal_error=True), FakeChild(2), FakeChild(3)]
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stderr(io.StringIO()):
            statepath = Path(directory) / 'state.json'
            self.assertFalse(local.cleanup(children, statepath, True, True))
            state = json.loads(statepath.read_text())
        self.assertTrue(all(child.signalled and child.waited for child in children))
        self.assertEqual(state['phase'], 'incomplete')
        self.assertEqual(state['exit_codes'], [None, 0, 0])
        self.assertTrue(any('simulated signal race' in error for error in state['cleanup_errors']))

    def test_wait_error_does_not_skip_later_children(self):
        children = [FakeChild(1, wait_error=True), FakeChild(2), FakeChild(3)]
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stderr(io.StringIO()):
            statepath = Path(directory) / 'state.json'
            self.assertFalse(local.cleanup(children, statepath, True, True))
            state = json.loads(statepath.read_text())
        self.assertTrue(all(child.waited for child in children))
        self.assertEqual(state['phase'], 'incomplete')
        self.assertTrue(any('simulated wait error' in error for error in state['cleanup_errors']))


class PrepareRootTest(unittest.TestCase):
    def test_explicit_root_creates_fresh_cluster(self):
        old_umask = os.umask(0o077)
        self.addCleanup(os.umask, old_umask)
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            (parent / 'target/debug').mkdir(parents=True)
            (parent / 'target/debug/yadbd').write_bytes(b'fake daemon')
            root = parent / 'cluster'
            with mock.patch.object(local, 'REPO', parent), mock.patch.object(local, 'check_ports'), \
                    mock.patch.object(local.subprocess, 'run') as build, contextlib.redirect_stdout(io.StringIO()):
                local.prepare(root)
            build.assert_called_once()
            self.assertEqual(root.stat().st_mode & 0o777, 0o700)
            self.assertEqual(local.load(root / 'state.json'), {'phase': 'prepared'})
            identity_path = root / 'config' / 'identity.json'
            identity = local.load(identity_path)
            self.assertEqual(identity_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(set(identity), {'http_base', 'node_ids', 'deployment_id', 'listener_sha256'})
            self.assertEqual(identity['node_ids'], [1, 2, 3])
            self.assertEqual(identity['listener_sha256'], local.listener_hash(root, 8420))
            self.assertNotIn('subject', identity_path.read_text())
            self.assertNotIn('secret', identity_path.read_text())
            self.assertNotIn('key', identity_path.read_text())
            self.assertEqual(len(list((root / 'config').glob('*-group-*.json'))), 9)
            self.assertEqual(len(list((root / 'config').glob('*-manifest.json'))), 3)
            for node in range(1, 4):
                manifest = local.load(root / 'config' / f'node-{node}-manifest.json')
                for group, config_path in enumerate(manifest['groups'], 1):
                    config = local.load(Path(config_path))
                    self.assertEqual(config['root'], str(root.resolve() / 'metadata'))
                    self.assertEqual(config['preferred_leader'], group)
                    self.assertEqual(config['mode'], 'create')

    def test_port_base_offsets_every_listener_and_is_recorded(self):
        old_umask = os.umask(0o077)
        self.addCleanup(os.umask, old_umask)
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            (parent / 'target/debug').mkdir(parents=True)
            (parent / 'target/debug/yadbd').write_bytes(b'fake daemon')
            root = parent / 'cluster'
            with mock.patch.object(local, 'REPO', parent), \
                    mock.patch.object(local, 'check_ports') as ports_check, \
                    mock.patch.object(local.subprocess, 'run'), contextlib.redirect_stdout(io.StringIO()):
                local.prepare(root, 9420)
            ports_check.assert_called_once_with(9420)
            self.assertEqual(local.deployment_ports(root), 9420)
            config = local.load(root / 'config' / 'node-1-group-1.json')
            self.assertEqual(config['listen'], '127.0.0.1:19420')
            self.assertEqual([peer['address'] for peer in config['peers']],
                             ['127.0.0.1:19420', '127.0.0.1:19421', '127.0.0.1:19422'])
            self.assertEqual(local.ports(9420)[:3], [9420, 9421, 9422])
            self.assertEqual(local.ports(9420)[-1], 19428)

    def test_port_base_must_leave_room_for_all_listeners(self):
        local.validate_http_base(55527)
        self.assertEqual(local.ports(55527)[-1], 65535)
        for bad in (1023, 55528, 55535, 65535):
            with self.subTest(base=bad), self.assertRaises(ValueError):
                local.validate_http_base(bad)
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(local, 'check_ports') as ports_check:
            with self.assertRaises(ValueError):
                local.prepare(Path(directory) / 'cluster', 55528)
            ports_check.assert_not_called()

    def test_cli_dispatches_port_base_and_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'cluster'
            with mock.patch.object(local, 'prepare') as prepare, \
                    mock.patch.object(local.sys, 'argv', ['yadb-local.py', 'prepare', '--root', str(root), '--port-base', '9420']):
                local.main()
            prepare.assert_called_once_with(root, 9420)
            with mock.patch.object(local, 'status', return_value=True) as status, \
                    mock.patch.object(Path, 'resolve', return_value=root), \
                    mock.patch.object(local.sys, 'argv', ['yadb-local.py', 'status', str(root)]):
                local.main()
            status.assert_called_once_with(root)

    def test_existing_file_directory_and_symlink_refused(self):
        old_umask = os.umask(0o077)
        self.addCleanup(os.umask, old_umask)
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(local, 'check_ports'), \
                mock.patch.object(local.subprocess, 'run') as build:
            parent = Path(directory)
            (parent / 'file').write_text('keep')
            (parent / 'directory').mkdir()
            (parent / 'link').symlink_to(parent / 'missing')
            for name in ('file', 'directory', 'link'):
                with self.subTest(name=name), self.assertRaises(FileExistsError):
                    local.prepare(parent / name)
            self.assertEqual((parent / 'file').read_text(), 'keep')
            self.assertFalse((parent / 'missing').exists())
            build.assert_not_called()

    def test_missing_parent_refused(self):
        old_umask = os.umask(0o077)
        self.addCleanup(os.umask, old_umask)
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(local, 'check_ports'):
            with self.assertRaises(FileNotFoundError):
                local.prepare(Path(directory) / 'missing' / 'cluster')


class IdentityTest(unittest.TestCase):
    def test_fail_closed_and_detect_listener_edits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / 'config'
            config.mkdir()
            ports = config / 'ports.json'
            identity_path = config / 'identity.json'
            ports.write_text(json.dumps({'http_base': 9420}))
            with self.assertRaisesRegex(ValueError, 'identity.json is missing'):
                local.deployment_ports(root)
            for node in range(1, 4):
                for group in range(1, 4):
                    (config / f'node-{node}-group-{group}.json').write_text(json.dumps({
                        'node': node, 'group': group, 'listen': 'original', 'peers': []}))
            identity = {'http_base': 9420, 'node_ids': [1, 2, 3], 'deployment_id': 'test',
                        'listener_sha256': local.listener_hash(root, 9420)}
            identity_path.write_text(json.dumps(identity))
            self.assertEqual(local.deployment_ports(root), 9420)
            ports.write_text(json.dumps({'http_base': 8420}))
            with self.assertRaisesRegex(ValueError, 'port bases disagree'):
                local.deployment_ports(root)
            ports.write_text(json.dumps({'http_base': 9420}))
            path = config / 'node-1-group-1.json'
            cfg = local.load(path)
            cfg['listen'] = 'edited'
            path.write_text(json.dumps(cfg))
            with self.assertRaisesRegex(ValueError, 'listener hash mismatch'):
                local.deployment_ports(root)
            identity['http_base'] = 55528
            identity_path.write_text(json.dumps(identity))
            with self.assertRaisesRegex(ValueError, 'identity port base is out of range'):
                local.deployment_ports(root)
            ports.unlink()
            with self.assertRaisesRegex(ValueError, 'ports.json is missing'):
                local.deployment_ports(root)

    def test_legacy_fallback_warns(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stderr(io.StringIO()) as output:
            self.assertEqual(local.deployment_ports(Path(directory)), 8420)
            self.assertIn('identity cannot be verified', output.getvalue())


class StatusTest(unittest.TestCase):
    def make_root(self, directory, state):
        root = Path(directory)
        (root / 'state.json').write_text(json.dumps(state))
        return root

    def run_status(self, root, alive, cmdlines, http, raft):
        with mock.patch.object(local, 'pid_alive', side_effect=lambda pid: alive.get(pid, False)), \
                mock.patch.object(local, 'pid_cmdline', side_effect=lambda pid: cmdlines.get(pid)), \
                mock.patch.object(local, 'http_health', side_effect=lambda port: http), \
                mock.patch.object(local, 'port_open', side_effect=lambda port: raft), \
                contextlib.redirect_stdout(io.StringIO()):
            return local.status(root)

    def test_running_state_requires_live_matching_processes_and_ports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.make_root(directory, {'phase': 'running', 'supervisor_pid': 10, 'pids': [11, 12, 13]})
            cmdlines = {10: ['python3', str(root / 'scripts/yadb-local.py'), 'run', str(root)]}
            cmdlines.update({pid: [str(root / 'bin/yadbd'), '--node-id', str(pid - 10)] for pid in (11, 12, 13)})
            self.assertTrue(self.run_status(root, {10: True, 11: True, 12: True, 13: True}, cmdlines, True, True))
            self.assertFalse(self.run_status(root, {10: True, 11: False, 12: True, 13: True}, cmdlines, True, True))

    def test_stopped_state_rejects_live_processes_or_listeners(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.make_root(directory, {'phase': 'stopped', 'pids': [11, 12, 13]})
            self.assertTrue(self.run_status(root, {}, {}, False, False))
            self.assertFalse(self.run_status(root, {11: True}, {}, False, False))
            self.assertFalse(self.run_status(root, {}, {}, True, False))

    def test_prepared_state_is_consistent_without_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.make_root(directory, {'phase': 'prepared'})
            self.assertTrue(self.run_status(root, {}, {}, False, False))
            self.assertFalse(self.run_status(root, {}, {}, False, True))
            self.assertFalse(self.run_status(root, {}, {}, True, False))


class LaunchCommandTest(unittest.TestCase):
    """The daemon argv is the only place the admin allowlist is provisioned.

    An OpenRaft daemon refuses to start without --admin-subject, so a missing or
    duplicated flag is a startup failure, not a degraded mode.
    """

    def test_node_command_provisions_exactly_one_admin_subject(self):
        command = local.node_command(Path('/deployment'), 9420, 2, '1=127.0.0.1:9420')
        self.assertEqual(command[0], '/deployment/bin/yadbd')
        self.assertEqual(command.count('--admin-subject'), 1)
        self.assertEqual(command[command.index('--admin-subject') + 1], local.OPERATOR)
        self.assertTrue(local.OPERATOR)

    def test_node_command_binds_each_node_to_its_own_ports_and_artifacts(self):
        root = Path('/deployment')
        peers = '1=127.0.0.1:9420,2=127.0.0.1:9421,3=127.0.0.1:9422'
        commands = [local.node_command(root, 9420, node, peers) for node in (1, 2, 3)]
        listens = [c[c.index('--listen') + 1] for c in commands]
        self.assertEqual(listens, ['127.0.0.1:9420', '127.0.0.1:9421', '127.0.0.1:9422'])
        for node, command in zip((1, 2, 3), commands):
            self.assertEqual(command[command.index('--node-id') + 1], str(node))
            self.assertEqual(command[command.index('--peers') + 1], peers)
            self.assertEqual(command[command.index('--metadata-runtime') + 1], 'openraft')
            self.assertEqual(command[command.index('--data-dir') + 1], f'/deployment/data/node-{node}')
            self.assertEqual(command[command.index('--experimental-openraft-config') + 1],
                             f'/deployment/config/node-{node}-manifest.json')

    def test_node_command_never_carries_the_subject_secret(self):
        command = local.node_command(Path('/deployment'), 8420, 1, '1=127.0.0.1:8420')
        self.assertNotIn('--subject-secret', command)
        self.assertFalse(any('YADB_SUBJECT_SECRET' in arg for arg in command))


class BackupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(os.umask, os.umask(0o077))
        parent = Path(self.temp.name)
        (parent / 'target/debug').mkdir(parents=True)
        (parent / 'target/debug/yadbd').write_text('#!/usr/bin/env python3\nimport json\nprint(json.dumps({"group": 1, "node": 1, "first_index": None, "last_index": None, "last_purged": None, "applied": None, "snapshot": None, "entry_count": 0, "torn_tail": False, "restorable": True, "reason": None, "membership_differs_from_descriptor": None}))\n')
        self.root = parent / 'cluster'
        self.dest = parent / 'backup'
        with mock.patch.object(local, 'REPO', parent), mock.patch.object(local, 'check_ports'), \
                mock.patch.object(local.subprocess, 'run'), contextlib.redirect_stdout(io.StringIO()):
            local.prepare(self.root)
        (self.root / 'state.json').write_text(json.dumps({'phase': 'stopped', 'pids': []}))
        for node in range(1, 4):
            (self.root / 'data' / f'node-{node}').mkdir()
            for group in range(1, 4):
                replica = self.root / 'metadata' / f'{node}-{group}'
                replica.mkdir()
                (replica / 'log').write_bytes(b'replica')

    def run_backup(self):
        with mock.patch.object(local, 'port_open', return_value=False), \
                mock.patch.object(local, 'http_health', return_value=False), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return local.backup(self.root, self.dest)

    def fake_verdict(self, **overrides):
        """Replace the stand-in daemon with one that prints a chosen verdict."""
        verdict = {'group': 1, 'node': 1, 'first_index': None, 'last_index': None,
                   'last_purged': None, 'applied': None, 'snapshot': None, 'entry_count': 0,
                   'torn_tail': False, 'restorable': True, 'reason': '',
                   'membership_differs_from_descriptor': None}
        verdict.update(overrides)
        script = ('#!/usr/bin/env python3\n'
                  'import json, sys\n'
                  f'verdict = json.loads({json.dumps(json.dumps(verdict))})\n'
                  'print(json.dumps(verdict))\n'
                  "if not verdict['restorable']:\n"
                  "    sys.stderr.write(verdict['reason'] + '\\n')\n"
                  '    sys.exit(4)\n')
        for path in (Path(self.temp.name) / 'target/debug/yadbd', self.root / 'bin/yadbd'):
            path.write_text(script)
            path.chmod(0o700)

    def test_existing_destination_untouched(self):
        self.dest.mkdir()
        (self.dest / 'keep').write_text('unchanged')
        self.assertEqual(self.run_backup(), 2)
        self.assertEqual([p.name for p in self.dest.iterdir()], ['keep'])
        self.assertEqual((self.dest / 'keep').read_text(), 'unchanged')

    def test_not_stopped_refused(self):
        (self.root / 'state.json').write_text('{"phase":"running"}')
        self.assertEqual(self.run_backup(), 2)
        self.assertFalse((self.root / '.yadb-backup-in-progress').exists())

    def test_existing_marker_refuses_and_creates_no_destination(self):
        # A destination that exists but carries neither COMPLETE nor INCOMPLETE
        # is indistinguishable from a damaged artifact. A refusal must not
        # create one.
        (self.root / '.yadb-backup-in-progress').write_text('earlier backup\n')
        self.assertEqual(self.run_backup(), 2)
        self.assertFalse(self.dest.exists())
        self.assertEqual((self.root / '.yadb-backup-in-progress').read_text(), 'earlier backup\n')

    def test_diagnostics_do_not_contaminate_stdout(self):
        out = io.StringIO()
        with mock.patch.object(local, 'port_open', return_value=False), \
                mock.patch.object(local, 'http_health', return_value=False), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(local.backup(self.root, self.dest), 0)
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertTrue(lines[0].startswith('Backup: '))
        self.assertTrue(lines[1].startswith('SHA-256: '))

    def test_copy_failure_retains_markers_and_incomplete(self):
        with mock.patch.object(local, 'backup_copy', side_effect=OSError('copy failed')):
            self.assertEqual(self.run_backup(), 5)
        self.assertTrue((self.root / '.yadb-backup-in-progress').exists())
        for node in range(1, 4):
            self.assertTrue((self.root / 'data' / f'node-{node}' / '.yadb-backup-in-progress').exists())
        self.assertIn('copy failed', (self.dest / 'INCOMPLETE').read_text())
        self.assertFalse((self.dest / 'COMPLETE').exists())

    def test_manifest_and_completion_hashes(self):
        self.assertEqual(self.run_backup(), 0)
        data = (self.dest / 'manifest.json').read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        self.assertEqual((self.dest / 'manifest.sha256').read_text().strip(), digest)
        self.assertEqual((self.dest / 'COMPLETE').read_text().strip(), digest)
        manifest = json.loads(data)
        payload = self.dest / 'payload'
        self.assertEqual({f['path'] for f in manifest['files']}, {p.relative_to(payload).as_posix() for p in payload.rglob('*') if p.is_file()})
        for entry in manifest['files']:
            path = payload / entry['path']
            self.assertEqual(entry['sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(entry['mode'], path.stat().st_mode & 0o7777)
        self.assertEqual(len(manifest['replicas']), 9)
        self.assertFalse((self.root / '.yadb-backup-in-progress').exists())

    def test_symlink_refused(self):
        (self.root / 'data/link').symlink_to(self.root / 'state.json')
        self.assertEqual(self.run_backup(), 4)
        self.assertTrue((self.dest / 'INCOMPLETE').exists())
        self.assertFalse((self.dest / 'COMPLETE').exists())

    def test_torn_log_tail_refused(self):
        # The copy would preserve the torn frame, and the next open truncates it,
        # so a published artifact would lose the final frame without saying so.
        self.fake_verdict(torn_tail=True)
        self.assertEqual(self.run_backup(), 4)
        self.assertTrue((self.dest / 'INCOMPLETE').exists())
        self.assertFalse((self.dest / 'COMPLETE').exists())
        self.assertIn('torn log tail', (self.dest / 'INCOMPLETE').read_text())

    def test_membership_difference_refused(self):
        self.fake_verdict(membership_differs_from_descriptor={
            'descriptor_voters': [1, 2, 3], 'stored_voters': [1, 2]})
        self.assertEqual(self.run_backup(), 4)
        self.assertTrue((self.dest / 'INCOMPLETE').exists())
        self.assertFalse((self.dest / 'COMPLETE').exists())

    def test_not_restorable_refused(self):
        self.fake_verdict(restorable=False, reason='applied log id absent from log')
        self.assertEqual(self.run_backup(), 4)
        self.assertIn('applied log id absent from log', (self.dest / 'INCOMPLETE').read_text())


class RunBackupGateTest(unittest.TestCase):
    def test_run_refuses_while_a_backup_marker_exists(self):
        # `run` reads the marker after it holds supervisor.lock. A `run` that
        # checked first and locked later would start a live process after a
        # backup had already written the marker, and the copy would race it.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'state.json').write_text(json.dumps({'phase': 'stopped'}))
            (root / '.yadb-backup-in-progress').write_text('backup\n')
            with self.assertRaises(RuntimeError) as raised:
                local.run(root)
            self.assertIn('.yadb-backup-in-progress', str(raised.exception))
            # The refusal must not rewrite lifecycle state as if a run happened.
            self.assertEqual(json.loads((root / 'state.json').read_text()), {'phase': 'stopped'})


if __name__ == '__main__':
    unittest.main()
