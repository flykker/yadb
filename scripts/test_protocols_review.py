"""Offline regression checks for protocol harness refusal assertions."""
import importlib.util
from pathlib import Path
import unittest


spec = importlib.util.spec_from_file_location(
    "protocols", Path(__file__).resolve().parents[1] / "tests" / "protocols.py")
protocols = importlib.util.module_from_spec(spec)
spec.loader.exec_module(protocols)


class SqlNoAckTest(unittest.TestCase):
    def test_error_response_without_command_complete(self):
        protocols.assert_sql_no_ack([(b"E", b"error"), (b"Z", b"I")])

    def test_worker_exception_is_not_a_refusal(self):
        error = AssertionError("unexpected SQL acknowledgement")
        with self.assertRaisesRegex(AssertionError, "unexpected SQL acknowledgement"):
            protocols.assert_sql_no_ack(error)

    def test_ack_even_with_error_is_rejected(self):
        with self.assertRaises(AssertionError):
            protocols.assert_sql_no_ack([(b"C", b"INSERT 0 1\0"), (b"E", b"error")])

    def test_missing_error_is_rejected(self):
        with self.assertRaises(AssertionError):
            protocols.assert_sql_no_ack([(b"Z", b"I")])


if __name__ == "__main__":
    unittest.main()
