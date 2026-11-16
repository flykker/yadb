"""Unit tests for scripts/yadb-openraft-dev.py lifecycle bookkeeping.

These cover the two failure paths a live drill cannot force reliably: a stop
that times out with only some nodes still running, and the replica-mode
reconciliation that every node start must perform before it spawns a daemon.
"""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


spec = importlib.util.spec_from_file_location(
    'yadb_openraft_dev', Path(__file__).with_name('yadb-openraft-dev.py'))
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)


def args_for(root, **extra):
    values = {'root': str(root)}
    values.update(extra)
    return mock.Mock(**values)


class StopTimeoutTest(unittest.TestCase):
    def test_stop_timeout_keeps_node_indexed_pid_slots(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'state.json').write_text(json.dumps({
                'phase': 'running', 'port_base': 23500,
                'pids': [111, 222, 333], 'down': []}))

            # Only node 2 refuses to exit. A compacted PID list would record
            # [222] and lose which node it belongs to, so every later lifecycle
            # operation would fail with a generic "expected 3 PIDs".
            alive = {111: False, 222: True, 333: False}

            with mock.patch.object(dev, 'resolve_root', return_value=root), \
                    mock.patch.object(dev, 'cluster_lock', mock.MagicMock()), \
                    mock.patch.object(dev, 'pid_alive', side_effect=lambda pid: alive[pid]), \
                    mock.patch.object(dev, 'is_our_daemon', return_value=True), \
                    mock.patch.object(dev.os, 'kill', mock.MagicMock()), \
                    mock.patch.object(dev.time, 'sleep', mock.MagicMock()), \
                    mock.patch.object(dev.time, 'monotonic',
                                      side_effect=[0, 0, 0, dev.STOP_TIMEOUT + 1,
                                                   dev.STOP_TIMEOUT + 1,
                                                   dev.STOP_TIMEOUT + 1]):
                with self.assertRaises(RuntimeError):
                    dev.cmd_stop(args_for(root))

            state = json.loads((root / 'state.json').read_text())
            self.assertEqual(state['pids'], [None, 222, None])
            self.assertEqual(state['phase'], 'stopping')
            self.assertIn('did not exit after SIGTERM', state['error'])


class StartNodeReplicaModeTest(unittest.TestCase):
    def _assert_sync_precedes_start(self, command, **extra):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calls = []
            with mock.patch.object(dev, 'resolve_root', return_value=root), \
                    mock.patch.object(dev, 'cluster_lock', mock.MagicMock()), \
                    mock.patch.object(dev, '_running_state', return_value={'pids': [None, None, None], 'port_base': 23500}), \
                    mock.patch.object(dev, 'sync_replica_modes',
                                      side_effect=lambda r: calls.append('sync')), \
                    mock.patch.object(dev, '_bring_up',
                                      side_effect=lambda r, s, n: calls.append('start')):
                command(args_for(root, **extra))
            # The first start writes every config in create mode; a node started
            # afterwards must reconcile from the directories on disk first, or
            # it is refused with "replica path conflicts with create/open mode".
            self.assertEqual(calls, ['sync', 'start'])

    def test_start_node_reconciles_replica_modes_first(self):
        self._assert_sync_precedes_start(dev.cmd_start_node, node=2)

    def test_restart_node_reconciles_replica_modes_first(self):
        self._assert_sync_precedes_start(dev.cmd_restart_node, node=2, signal='TERM')


if __name__ == '__main__':
    unittest.main()
