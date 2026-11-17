"""Offline contract checks for the Multi-Raft protocol refusal assertions."""
import json
import unittest
from unittest.mock import patch

import protocols


class GcRefusalProtocolTest(unittest.TestCase):
    def check_response(self, status=409, reason="mark-and-sweep disabled for Multi-Raft",
                       deleted=0, reclaimed=0, fragment=(200, {}, b"seed"),
                       path="/v1/pools/gc", scrub_error="scrub disabled for Multi-Raft"):
        summary = {"gc_refusal": reason, "fragments_deleted": deleted,
                   "bytes_reclaimed": reclaimed, "orphan_collection_error": scrub_error,
                   "orphan_blobs_collected": 0}
        response = (status, {}, json.dumps(summary).encode())
        with patch.object(protocols, "request", side_effect=[response, fragment]) as request, \
             patch.object(protocols, "admin_headers", return_value={"admin": "yes"}):
            protocols.assert_multiraft_gc_refusal(123, path, [(456, "/fragment", b"seed")])
            self.assertEqual(request.call_count, 2)
            self.assertEqual(request.call_args_list[0].args, (123, "POST", path))
            self.assertEqual(request.call_args_list[1].args, (456, "GET", "/fragment"))

    def test_both_routes_accept_explicit_refusal_and_intact_fragment(self):
        for path in ("/v1/pools/gc", "/v1/pools/scrub"):
            with self.subTest(path=path):
                self.check_response(path=path)

    def test_rejects_success_wrong_reason_or_nonzero_counters(self):
        for kwargs in ({"status": 200}, {"reason": None}, {"reason": "lease busy"},
                       {"deleted": 1}, {"reclaimed": 1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(AssertionError):
                self.check_response(**kwargs)

    def test_rejects_missing_or_changed_fragment(self):
        for fragment in ((404, {}, b""), (200, {}, b"changed")):
            with self.subTest(fragment=fragment), self.assertRaises(AssertionError):
                self.check_response(fragment=fragment)

    def test_scrub_requires_repair_refusal(self):
        with self.assertRaises(AssertionError):
            self.check_response(path="/v1/pools/scrub", scrub_error=None)


if __name__ == "__main__":
    unittest.main()
