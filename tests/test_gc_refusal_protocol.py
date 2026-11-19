"""Offline contract checks for the Multi-Raft per-group refusal assertions.

The whole-deployment refusal this file used to cover no longer exists: the
scrub runs its repair scan one metadata group at a time, so the only refusal
left is "this node does not lead that group". `assert_multiraft_repair_scan` is
the assertion that pins that contract, and these tests exercise it without a
cluster by mocking the election and the HTTP surface.

The scopes are the *data* groups that own a project (`gc_group_scopes`), not
every Raft group, so the totals in a refusal are counted over two groups here,
and a node that leads no data group refuses both.
"""
import json
import unittest
from unittest.mock import patch

import protocols

DEFAULT = object()


class RepairScanProtocolTest(unittest.TestCase):
    NODE = 2

    def leaders(self, group, nodes=None):
        """Node 2 leads the billing group and nothing else."""
        return {2: self.NODE, 3: 3}[group]

    def summary(self, status=409, refusal=DEFAULT, sweep=DEFAULT,
                orphan_error=None, checked=0):
        if refusal is DEFAULT:
            refusal = (
                "repair scan refused for 1 of 2 metadata group(s): "
                "group 3 refused: node 2 is not the leader of metadata "
                "group 3 (leader is Some(3))")
        if sweep is DEFAULT:
            sweep = (
                "garbage collection refused for 1 of 2 metadata group(s): "
                "group 3 refused: node 2 is not the leader of metadata group 3")
        body = {"repair_error": refusal, "gc_refusal": sweep,
                "orphan_collection_error": orphan_error,
                "orphan_blobs_collected": 0, "blobs_checked": checked}
        return status, {}, json.dumps(body).encode()

    def check(self, response=None, fragment=(200, {}, b"seed"), **kwargs):
        response = self.summary(**kwargs) if response is None else response
        with patch.object(protocols, "wait_group_leader", side_effect=self.leaders), \
             patch.object(protocols, "request",
                          side_effect=[response, fragment]) as request, \
             patch.object(protocols, "admin_headers", return_value={"admin": "yes"}):
            summary = protocols.assert_multiraft_repair_scan(
                123, self.NODE, [(456, "/fragment", b"seed")])
            return request, summary

    def test_accepts_a_per_group_refusal_and_proves_the_fragment(self):
        request, summary = self.check()
        self.assertEqual(request.call_count, 2)
        self.assertEqual(request.call_args_list[0].args,
                         (123, "POST", "/v1/pools/scrub"))
        self.assertEqual(request.call_args_list[0].kwargs["headers"], {"admin": "yes"})
        self.assertEqual(request.call_args_list[1].args, (456, "GET", "/fragment"))
        self.assertIn("x-yadb-raft-token",
                      request.call_args_list[1].kwargs["headers"])
        self.assertEqual(summary["blobs_checked"], 0)

    def test_accepts_a_named_group_in_the_orphan_error(self):
        self.check(orphan_error="group 3: node is not Raft leader")

    def test_a_node_that_leads_no_data_group_refuses_every_scope(self):
        # The control group's leader owns no project, so it is not a scope; the
        # two data groups are, and this node leads neither of them.
        with patch.object(protocols, "wait_group_leader",
                          side_effect=lambda group, nodes=None: {2: 2, 3: 3}[group]):
            with patch.object(protocols, "request",
                              side_effect=[self.summary(refusal=(
                                  "repair scan refused for 2 of 2 metadata "
                                  "group(s): group 2 refused: not the leader; "
                                  "group 3 refused: not the leader"),
                                  sweep=(
                                  "garbage collection refused for 2 of 2 "
                                  "metadata group(s): group 2 refused: not the "
                                  "leader; group 3 refused: not the leader"))]) as request, \
                 patch.object(protocols, "admin_headers", return_value={"admin": "yes"}):
                summary = protocols.assert_multiraft_repair_scan(123, 1)
        self.assertEqual(request.call_count, 1)
        self.assertIn("2 of 2", summary["repair_error"])

    def test_accepts_a_node_that_leads_every_data_group(self):
        with patch.object(protocols, "wait_group_leader",
                          side_effect=lambda group, nodes=None: self.NODE):
            with patch.object(protocols, "request",
                              side_effect=[self.summary(status=200, refusal=None,
                                                        sweep=None)]) as request, \
                 patch.object(protocols, "admin_headers", return_value={"admin": "yes"}):
                summary = protocols.assert_multiraft_repair_scan(123, self.NODE)
        self.assertEqual(request.call_count, 1)
        self.assertIsNone(summary["repair_error"])

    def test_rejects_a_success_status_that_still_refuses(self):
        with self.assertRaises(AssertionError):
            self.check(status=200)

    def test_rejects_a_refusal_that_names_the_group_this_node_leads(self):
        with self.assertRaises(AssertionError):
            self.check(refusal=(
                "repair scan refused for 1 of 2 metadata group(s): "
                "group 2 refused: not the leader"))

    def test_rejects_a_refusal_that_names_no_refused_group(self):
        with self.assertRaises(AssertionError):
            self.check(refusal=(
                "repair scan refused for 1 of 2 metadata group(s): "
                "the view was not provable"))

    def test_rejects_a_refusal_counted_over_the_wrong_total(self):
        # Every Raft group is not a scope: the control group owns no project,
        # so "1 of 3" cannot come from this deployment.
        with self.assertRaises(AssertionError):
            self.check(refusal=(
                "repair scan refused for 1 of 3 metadata group(s): "
                "group 3 refused: not the leader"))

    def test_rejects_a_missing_refusal(self):
        with self.assertRaises(AssertionError):
            self.check(refusal="")

    def test_rejects_a_missing_sweep_refusal(self):
        with self.assertRaises(AssertionError):
            self.check(sweep="")

    def test_rejects_an_orphan_error_that_names_no_group(self):
        with self.assertRaises(AssertionError):
            self.check(orphan_error="whole-deployment view is unavailable")

    def test_rejects_an_extra_refused_group_name(self):
        # The body names the group it should refuse and then an extra one that
        # is not a scope at all. Set comparison is what makes this fail: a
        # one-name-at-a-time presence check would accept it.
        with self.assertRaises(AssertionError):
            self.check(refusal=(
                "repair scan refused for 1 of 2 metadata group(s): "
                "group 3 refused: not the leader; "
                "group 99 refused: not the leader"))

    def test_rejects_an_extra_group_name_in_the_sweep_refusal(self):
        with self.assertRaises(AssertionError):
            self.check(sweep=(
                "garbage collection refused for 1 of 2 metadata group(s): "
                "group 3 refused: not the leader; "
                "group 99 refused: not the leader"))

    def test_rejects_a_fragment_that_disappeared_or_changed(self):
        for fragment in ((404, {}, b""), (200, {}, b"changed")):
            with self.subTest(fragment=fragment), self.assertRaises(AssertionError):
                self.check(fragment=fragment)

    def test_rejects_fewer_checked_blobs_than_the_caller_knows(self):
        with self.assertRaises(AssertionError):
            with patch.object(protocols, "wait_group_leader",
                              side_effect=self.leaders), \
                 patch.object(protocols, "request",
                              side_effect=[self.summary(checked=1)]) as request, \
                 patch.object(protocols, "admin_headers", return_value={"admin": "yes"}):
                protocols.assert_multiraft_repair_scan(123, self.NODE,
                                                       checked_at_least=2)


if __name__ == "__main__":
    unittest.main()
