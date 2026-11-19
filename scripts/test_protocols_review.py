"""Offline regression checks for protocol harness refusal assertions and SKIPs.

The second class parses `tests/protocols.py` with `ast` instead of importing it.
Every `skip("scenario", "reason")` must be a literal (or an f-string registered
below), and every `path:line` its reason cites must still resolve to the code
the reason quotes.  A SKIP is a recorded absence of evidence, never a pass, so
it must not be able to disappear or rot silently.
"""
import ast
import contextlib
import importlib.util
import io
from pathlib import Path
import re
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = REPO_ROOT / "tests" / "protocols.py"


spec = importlib.util.spec_from_file_location("protocols", HARNESS_PATH)
protocols = importlib.util.module_from_spec(spec)
spec.loader.exec_module(protocols)


# A citation as it appears in a SKIP reason, e.g. crates/yadbd/src/main.rs:524.
# It is also used with fullmatch() for the entries of EXPECTED_SKIPS.
ANCHOR_RE = re.compile(r"([A-Za-z0-9_./-]+\.(?:rs|py)):(\d+)")

# What the cited line must still say.  A citation without an entry here is
# rejected: an unverifiable anchor is not allowed to pass.
REQUIRED_SUBSTRINGS = {
    "crates/yadbd/src/protocols/s3.rs:683": ["is_openraft"],
    "crates/yadbd/src/main.rs:524": ["begin_read_lease"],
    "crates/yadbd/src/main.rs:7636": ["CONFLICT"],
    "crates/yadbd/src/gc.rs:149": ["MULTIRAFT_GC_REFUSAL"],
}

# Every SKIP in tests/protocols.py, with the anchors its reason must keep.
# An anchor is either a `path:line` citation (resolved against the repo root
# and checked against REQUIRED_SUBSTRINGS) or a literal phrase that must still
# appear in the reason.  Adding a SKIP means adding it here: the scenario set
# is compared exactly, in both directions.
EXPECTED_SKIPS = {
    "VDisk orphan collection": [
        "repair/orphan scan is disabled on Multi-Raft",
        "crates/yadbd/src/main.rs:7636",
    ],
    "per-group GC collection": [
        "mark-and-sweep is disabled for Multi-Raft",
        "crates/yadbd/src/gc.rs:149",
    ],
    "reader lease": [
        "crates/yadbd/src/protocols/s3.rs:683",
        "crates/yadbd/src/main.rs:524",
        "Multi-Raft has no fenced read lease",
    ],
    "cross-process InstallSnapshot": [
        "Config::default",
        "since_last:5000",
        "--install-snapshot-live",
    ],
    "cross-process InstallSnapshot (attempted)": [
        "did not reach the daemon's default snapshot trigger",
        "no install was attempted",
        "no PASS is claimed",
    ],
}

# The two InstallSnapshot reasons interpolate runtime numbers, so they are
# f-strings.  They may not be checked as one literal, so each is registered
# here and its static text is checked against EXPECTED_SKIPS instead.  Any
# other f-string (or any non-string expression) is an unverifiable SKIP and is
# rejected by collect_skips().
DYNAMIC_SKIP_SCENARIOS = {
    "cross-process InstallSnapshot",
    "cross-process InstallSnapshot (attempted)",
}


class UnverifiableSkip(Exception):
    """A SKIP whose scenario or reason cannot be checked statically."""


def literal_string(node):
    """Return the value of a plain string-literal node, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def static_text(node):
    """Return (text, is_literal) for a string literal or f-string.

    Formatted values are replaced by `{}` so the constant fragments can still
    be searched for citations and phrases.  Returns (None, False) for anything
    that is not a string expression at all (a name, a call, a concat of
    non-strings, ...).
    """
    text = literal_string(node)
    if text is not None:
        return text, True
    if isinstance(node, ast.JoinedStr):
        pieces = []
        for part in node.values:
            literal = literal_string(part)
            pieces.append(literal if literal is not None else "{}")
        return "".join(pieces), False
    return None, False


def collect_skips(tree, allowed_dynamic=DYNAMIC_SKIP_SCENARIOS):
    """Return (literal_skips, dynamic_skips) from a parsed harness.

    Raises UnverifiableSkip for a call whose scenario is not a literal, whose
    reason is neither a literal nor a registered f-string, or that does not
    take exactly two positional arguments.  A SKIP that cannot be checked is
    not allowed.
    """
    literal_skips = {}
    dynamic_skips = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "skip"):
            continue
        if len(node.args) != 2 or node.keywords:
            raise UnverifiableSkip(
                f"line {node.lineno}: skip() must take exactly two positional "
                f"arguments so its scenario and reason can be checked")
        scenario = literal_string(node.args[0])
        if scenario is None:
            raise UnverifiableSkip(
                f"line {node.lineno}: SKIP scenario is not a string literal")
        reason, is_literal = static_text(node.args[1])
        if reason is None:
            raise UnverifiableSkip(
                f"line {node.lineno}: SKIP {scenario!r} reason is not a string "
                f"literal, so its anchors cannot be checked")
        if not is_literal and scenario not in allowed_dynamic:
            raise UnverifiableSkip(
                f"line {node.lineno}: SKIP {scenario!r} reason is an f-string "
                f"and the scenario is not registered in DYNAMIC_SKIP_SCENARIOS")
        target = literal_skips if is_literal else dynamic_skips
        if scenario in target:
            raise UnverifiableSkip(
                f"line {node.lineno}: duplicate SKIP scenario {scenario!r}")
        target[scenario] = reason
    return literal_skips, dynamic_skips


def find_citations(reason):
    """Return every (path, line) citation in a reason, in source order."""
    return [(match.group(1), int(match.group(2)))
            for match in ANCHOR_RE.finditer(reason)]


def citation_key(path, line):
    return f"{path}:{line}"


def cited_line(path, line, root=REPO_ROOT):
    """Return the text of `path:line`, or raise AssertionError.

    The file must exist inside the repository, have at least `line` lines, and
    the cited line must not be blank.
    """
    relative = Path(path)
    if relative.is_absolute():
        raise AssertionError(
            f"{path}: citation must be relative to the repo root")
    root = Path(root).resolve()
    target = (root / relative).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        raise AssertionError(
            f"{path}: citation escapes the repository") from None
    if not target.is_file():
        raise AssertionError(f"{path}: cited file does not exist")
    lines = target.read_text().splitlines()
    if line < 1 or len(lines) < line:
        raise AssertionError(
            f"{path}:{line}: file has only {len(lines)} lines")
    text = lines[line - 1]
    if not text.strip():
        raise AssertionError(f"{path}:{line}: cited line is blank")
    return text


def check_reason_anchors(anchors, reason, *, root=REPO_ROOT,
                         required=REQUIRED_SUBSTRINGS):
    """Check one SKIP reason against its anchor list; return the citations.

    `anchors` is the EXPECTED_SKIPS entry: `path:line` strings are resolved and
    their required substrings verified, every other string must still occur in
    the reason.  The citation set must match exactly, so neither a removed nor
    an added citation can slip through.
    """
    expected = []
    phrases = []
    for anchor in anchors:
        match = ANCHOR_RE.fullmatch(anchor)
        if match:
            expected.append((match.group(1), int(match.group(2))))
        else:
            phrases.append(anchor)
    found = find_citations(reason)
    if sorted(found) != sorted(expected):
        raise AssertionError(
            "cited anchors changed: expected %s, reason cites %s"
            % (sorted(citation_key(*item) for item in expected),
               sorted(citation_key(*item) for item in found)))
    for path, line in expected:
        text = cited_line(path, line, root)
        key = citation_key(path, line)
        if key not in required:
            raise AssertionError(
                f"{key}: no required_substrings recorded, so the cited line "
                f"cannot be verified")
        for needle in required[key]:
            if needle not in text:
                raise AssertionError(
                    f"{key}: cited line no longer contains {needle!r}: "
                    f"{text.strip()!r}")
    for phrase in phrases:
        if phrase not in reason:
            raise AssertionError(f"reason no longer contains {phrase!r}")
    return expected


def pass_claims(text):
    """Return the `PASS` occurrences that are claims, not negations."""
    claims = []
    for match in re.finditer(r"\bPASS\b", text):
        prefix = text[:match.start()]
        if not re.search(r"\b(?:no|not|never)\s+$", prefix):
            claims.append(match.group(0))
    return claims


def assigned_constant(tree, name):
    """Return the value node assigned to a module-level NAME, else None."""
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return node.value
    return None


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


class SkipAnchorGuardTest(unittest.TestCase):
    """Every SKIP is inventoried, literal, and still points at its evidence."""

    @classmethod
    def setUpClass(cls):
        cls.source = HARNESS_PATH.read_text()
        cls.tree = ast.parse(cls.source)
        cls.literal_skips, cls.dynamic_skips = collect_skips(cls.tree)

    def reason(self, scenario):
        if scenario in self.literal_skips:
            return self.literal_skips[scenario]
        if scenario in self.dynamic_skips:
            return self.dynamic_skips[scenario]
        self.fail(f"SKIP {scenario!r} disappeared from tests/protocols.py")

    def skip_function(self):
        for node in self.tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == "skip":
                return node
        self.fail("tests/protocols.py no longer defines skip()")

    def test_nonliteral_skip_reason_is_rejected(self):
        tree = ast.parse('skip("probe", reason)\n')
        with self.assertRaisesRegex(UnverifiableSkip, "not a string literal"):
            collect_skips(tree)

    def test_unregistered_fstring_skip_reason_is_rejected(self):
        tree = ast.parse('skip("probe", f"value {x}")\n')
        with self.assertRaisesRegex(UnverifiableSkip, "DYNAMIC_SKIP_SCENARIOS"):
            collect_skips(tree)

    def test_collected_skip_scenarios_match_expected_exactly(self):
        collected = set(self.literal_skips) | set(self.dynamic_skips)
        missing = sorted(set(EXPECTED_SKIPS) - collected)
        unlisted = sorted(collected - set(EXPECTED_SKIPS))
        self.assertEqual(
            (missing, unlisted), ([], []),
            f"SKIP inventory changed: removed={missing}, unlisted={unlisted}")

    def test_only_registered_scenarios_use_fstring_reasons(self):
        self.assertEqual(sorted(self.dynamic_skips),
                         sorted(DYNAMIC_SKIP_SCENARIOS))
        self.assertEqual(
            sorted(self.literal_skips),
            sorted(set(EXPECTED_SKIPS) - DYNAMIC_SKIP_SCENARIOS))

    def test_cited_anchor_set_is_exactly_the_expected_set(self):
        for scenario, anchors in EXPECTED_SKIPS.items():
            expected = [anchor for anchor in anchors
                        if ANCHOR_RE.fullmatch(anchor)]
            found = [citation_key(path, line)
                     for path, line in find_citations(self.reason(scenario))]
            with self.subTest(scenario=scenario):
                self.assertEqual(sorted(found), sorted(expected),
                                 f"{scenario}: cited anchors changed")

    def test_every_cited_anchor_resolves_to_a_nonempty_line(self):
        for scenario in EXPECTED_SKIPS:
            for path, line in find_citations(self.reason(scenario)):
                with self.subTest(
                        scenario=scenario, anchor=citation_key(path, line)):
                    self.assertTrue(cited_line(path, line).strip())

    def test_cited_anchor_lines_still_contain_required_code(self):
        for key, needles in REQUIRED_SUBSTRINGS.items():
            path, line = key.rsplit(":", 1)
            text = cited_line(path, int(line))
            for needle in needles:
                with self.subTest(anchor=key, needle=needle):
                    self.assertIn(needle, text,
                                  f"{key}: cited line is {text.strip()!r}")

    def test_required_substrings_cover_every_expected_citation(self):
        for scenario, anchors in EXPECTED_SKIPS.items():
            for anchor in anchors:
                if ANCHOR_RE.fullmatch(anchor):
                    with self.subTest(scenario=scenario, anchor=anchor):
                        self.assertIn(
                            anchor, REQUIRED_SUBSTRINGS,
                            f"{scenario}: {anchor} has no required substrings, "
                            f"so a moved line could pass")

    def test_every_skip_reason_still_contains_its_text_anchors(self):
        for scenario, anchors in EXPECTED_SKIPS.items():
            reason = self.reason(scenario)
            for anchor in anchors:
                if ANCHOR_RE.fullmatch(anchor):
                    continue
                with self.subTest(scenario=scenario, anchor=anchor):
                    self.assertIn(anchor, reason)

    def test_literal_skip_reasons_never_mention_pass(self):
        for scenario, reason in self.literal_skips.items():
            with self.subTest(scenario=scenario):
                self.assertNotIn("PASS", reason,
                                 f"{scenario}: a SKIP must not claim success")

    def test_dynamic_skip_reasons_never_claim_a_pass(self):
        for scenario, reason in self.dynamic_skips.items():
            with self.subTest(scenario=scenario):
                self.assertEqual(pass_claims(reason), [],
                                 f"{scenario}: a SKIP must not claim success")

    def test_install_snapshot_skip_cites_the_trigger_and_opt_in_flag(self):
        reason = self.reason("cross-process InstallSnapshot")
        for anchor in ("Config::default", "since_last:5000",
                       "--install-snapshot-live"):
            with self.subTest(anchor=anchor):
                self.assertIn(anchor, reason)
        burst = assigned_constant(self.tree, "SNAPSHOT_BURST_ENTRIES")
        self.assertIsNotNone(burst, "SNAPSHOT_BURST_ENTRIES is gone")
        self.assertEqual(ast.literal_eval(burst), 5000)

    def test_skip_function_appends_to_skips_list(self):
        appended = [
            node for node in ast.walk(self.skip_function())
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "append"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "SKIPS"
        ]
        self.assertTrue(appended, "skip() no longer appends to SKIPS")

    def test_skip_records_and_prints_instead_of_counting_a_pass(self):
        buffer = io.StringIO()
        before = list(protocols.SKIPS)
        try:
            with contextlib.redirect_stdout(buffer):
                protocols.skip("guard probe", "guard probe reason")
            recorded = list(protocols.SKIPS)
        finally:
            protocols.SKIPS[:] = before
        self.assertIn(("guard probe", "guard probe reason"), recorded)
        self.assertIn("SKIP guard probe: guard probe reason", buffer.getvalue())
        self.assertNotIn("PASS", buffer.getvalue())

    def test_anchor_checker_rejects_a_wrong_line_number(self):
        # An in-memory copy of the reader-lease citations with the gate line
        # moved to the top of the file; it must not be accepted.
        reason = ("the gate is at crates/yadbd/src/protocols/s3.rs:1 and the "
                  "lease is at crates/yadbd/src/main.rs:524")
        with self.assertRaisesRegex(AssertionError, "cited anchors changed"):
            check_reason_anchors(
                ["crates/yadbd/src/protocols/s3.rs:683",
                 "crates/yadbd/src/main.rs:524"], reason)

    def test_anchor_checker_rejects_a_line_that_lost_its_code(self):
        # The cited lines are right, but the quoted code is gone: the
        # required-substring table must notice even though the lines exist.
        reason = ("crates/yadbd/src/protocols/s3.rs:683 and "
                  "crates/yadbd/src/main.rs:524")
        required = {
            "crates/yadbd/src/protocols/s3.rs:683": ["definitely_gone"],
            "crates/yadbd/src/main.rs:524": ["begin_read_lease"],
        }
        with self.assertRaisesRegex(AssertionError, "no longer contains"):
            check_reason_anchors(
                ["crates/yadbd/src/protocols/s3.rs:683",
                 "crates/yadbd/src/main.rs:524"],
                reason, required=required)

    def test_anchor_checker_accepts_the_real_reason(self):
        for scenario, anchors in EXPECTED_SKIPS.items():
            with self.subTest(scenario=scenario):
                check_reason_anchors(anchors, self.reason(scenario))


if __name__ == "__main__":
    unittest.main()
