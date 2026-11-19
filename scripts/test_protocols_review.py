"""Offline regression checks for protocol harness refusal assertions and SKIPs.

The harness no longer records a SKIP: every scenario runs.  The SKIP guard still
parses `tests/protocols.py` with `ast` instead of importing it, so a SKIP cannot
be reintroduced silently.  A SKIP is a recorded absence of evidence, never a
pass, so if one comes back it must be inventoried in `EXPECTED_SKIPS` and every
`path:line` its reason cites must still resolve to the code the reason quotes.
"""
import ast
import importlib.util
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
    # The reader-lease drill waits out this TTL; the constant is load-bearing
    # for the inversion it asserts, and the drill cites it.
    "crates/yadb-metakv/src/gc.rs:74": ["DEFAULT_TTL_MS"],
    "crates/yadb-s3/src/lib.rs:21": ["INTENT_GRACE_SECS"],
}

# Every SKIP in tests/protocols.py, with the anchors its reason must keep.
# An anchor is either a `path:line` citation (resolved against the repo root
# and checked against REQUIRED_SUBSTRINGS) or a literal phrase that must still
# appear in the reason.  Adding a SKIP means adding it here: the scenario set
# is compared exactly, in both directions.
#
# The last entry, `reader lease`, closed when Multi-Raft grew per-group reader
# leases; `assert_reader_lease_drill` now proves the lease and its expiry.  The
# mapping stays, empty, so a reintroduced SKIP is a failure until it is
# inventoried here.
EXPECTED_SKIPS = {}

# No SKIP reason is built from an f-string today.  The set stays empty so the
# mechanism that would allow one is still exercised by its own tests: an
# f-string SKIP that is not registered here is rejected as unverifiable.
DYNAMIC_SKIP_SCENARIOS = set()


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


class ReaderLeaseTtlTest(unittest.TestCase):
    """The harness's wait must not fall behind the shipped lease lifetime.

    `drain_reader_lease` sleeps for `READER_LEASE_TTL`, and the repair drill
    relies on the leader's lease having lapsed by then. If the Rust constant
    were raised, the wait would silently become too short and the drill would
    flake instead of failing for a reason a reader can see.
    """

    def test_harness_ttl_matches_the_shipped_constant(self):
        source = (REPO_ROOT / "crates" / "yadb-metakv" / "src" / "gc.rs").read_text()
        match = re.search(r"DEFAULT_TTL_MS:\s*u64\s*=\s*([\d_]+)", source)
        self.assertIsNotNone(match, "DEFAULT_TTL_MS is no longer a u64 literal")
        shipped_ms = int(match.group(1).replace("_", ""))
        self.assertEqual(int(protocols.READER_LEASE_TTL * 1000), shipped_ms)


class SkipAnchorGuardTest(unittest.TestCase):
    """No SKIP remains; a reintroduced one is inventoried and still verifiable."""

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

    def test_nonliteral_skip_reason_is_rejected(self):
        tree = ast.parse('skip("probe", reason)\n')
        with self.assertRaisesRegex(UnverifiableSkip, "not a string literal"):
            collect_skips(tree)

    def test_literal_skip_collection_accepts_a_well_formed_skip(self):
        # The harness defines no skip() any more, but the checker is still
        # exercised on in-memory source: a reintroduced SKIP must stay
        # collectable and inventoried.
        tree = ast.parse('skip("probe", "a reason")\n')
        self.assertEqual(collect_skips(tree), ({"probe": "a reason"}, {}))

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

    def test_install_snapshot_is_no_longer_skipped(self):
        # It used to be an opt-in SKIP because the default trigger needs a
        # 5000-entry burst. The harness now provisions the snapshot knobs, so
        # the scenario runs every time; a SKIP reappearing here would mean the
        # capability silently regressed to "not run".
        names = set(EXPECTED_SKIPS) | set(self.dynamic_skips)
        self.assertFalse([name for name in names if "InstallSnapshot" in name],
                         names)
        tuning = assigned_constant(self.tree, "SNAPSHOT_LIVE_TUNING")
        self.assertIsNotNone(tuning, "SNAPSHOT_LIVE_TUNING is gone")
        self.assertEqual(ast.literal_eval(tuning),
                         {"snapshot_after_logs": 128,
                          "max_in_snapshot_log_to_keep": 1})

    def test_skip_helper_presence_matches_the_inventory(self):
        # The helper is dead code while no SKIP is registered, so it is gone.
        # A registered SKIP must bring it back, or the harness would NameError
        # when the scenario runs.
        names = {node.name for node in self.tree.body
                 if isinstance(node, ast.FunctionDef)}
        self.assertEqual("skip" in names,
                         bool(EXPECTED_SKIPS or DYNAMIC_SKIP_SCENARIOS))

    def test_anchor_checker_rejects_a_wrong_line_number(self):
        # An in-memory reason that cites one line where another was expected;
        # it must not be accepted.
        reason = ("the TTL is at crates/yadb-metakv/src/gc.rs:75 and the "
                  "constant is at crates/yadb-metakv/src/gc.rs:74")
        with self.assertRaisesRegex(AssertionError, "cited anchors changed"):
            check_reason_anchors(
                ["crates/yadb-metakv/src/gc.rs:74",
                 "crates/yadb-metakv/src/gc.rs:76"], reason)

    def test_anchor_checker_rejects_a_line_that_lost_its_code(self):
        # The cited lines are right, but the quoted code is gone: the
        # required-substring table must notice even though the lines exist.
        reason = ("crates/yadb-metakv/src/gc.rs:74 and "
                  "crates/yadb-metakv/src/gc.rs:76")
        required = {
            "crates/yadb-metakv/src/gc.rs:74": ["DEFAULT_TTL_MS"],
            "crates/yadb-metakv/src/gc.rs:76": ["definitely_gone"],
        }
        with self.assertRaisesRegex(AssertionError, "no longer contains"):
            check_reason_anchors(
                ["crates/yadb-metakv/src/gc.rs:74",
                 "crates/yadb-metakv/src/gc.rs:76"],
                reason, required=required)

    def test_anchor_checker_accepts_a_matching_reason(self):
        # `EXPECTED_SKIPS` is empty, so the accept path is exercised on
        # in-memory source instead of being vacuously untested.
        reason = "the lease TTL is at crates/yadb-metakv/src/gc.rs:74"
        self.assertEqual(
            check_reason_anchors(
                ["crates/yadb-metakv/src/gc.rs:74"], reason),
            [("crates/yadb-metakv/src/gc.rs", 74)])

    def test_anchor_checker_accepts_the_real_reason(self):
        for scenario, anchors in EXPECTED_SKIPS.items():
            with self.subTest(scenario=scenario):
                check_reason_anchors(anchors, self.reason(scenario))


if __name__ == "__main__":
    unittest.main()
