"""Offline checks that repo-relative `file:line` citations in `docs/` resolve.

A doc that says "the check lives at `crates/yadbd/src/main.rs:7350`" is making a
verifiable claim. When the cited line moves, the claim silently becomes false and
a reader who follows the citation finds unrelated code. Four such citations had
rotted before this guard existed, so the drift is not hypothetical.

Scope, deliberately narrow: only citations written with a repository-relative
path (`crates/…`, `tests/…`, `scripts/…`) are checked. A bare `main.rs:928` is
ambiguous by construction - there are several `main.rs` files and the doc does not
say which - so it is not treated as verified evidence. That is a limitation of
this guard, not a claim that such citations are correct.

Two classes are checked differently:

* a **current claim** names a line that must still contain the code the sentence
  describes, so its expected substrings are recorded in `CURRENT_CLAIMS` and
  verified;
* a **historical snapshot** records what a past run observed. Its line numbers
  describe a tree that no longer exists, so only the file and the line range are
  checked, and the citation must be listed in `HISTORICAL_ANCHORS` - inside a
  section the doc explicitly dates.

The citation set is compared in both directions against those two tables, so a
citation that is added, removed or reclassified fails the check. That is
deliberate: adding a citation means deciding which class it belongs to.

Run directly: `python3 scripts/test_doc_anchors.py`.
"""
from pathlib import Path
import re
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS = REPO_ROOT / "docs"

# Only repo-relative paths: see the module docstring for why bare filenames are
# out of scope. `path:12` and `path:12-34` are both accepted.
CITATION_RE = re.compile(
    r"((?:crates|tests|scripts)/[A-Za-z0-9_./-]+\.(?:rs|py|md)):(\d+)(?:-(\d+))?")

# Current claims: the cited line must still contain every listed substring.
# A citation with no entry here must be a historical snapshot instead.
CURRENT_CLAIMS = {
    "crates/yadbd/src/main.rs:80": ["value_parser", "openraft"],
    "crates/yadbd/src/main.rs:8396": ["require_admin"],
    "crates/yadb-metakv/src/gc.rs:790": ["acquire_writer", "map(Some)"],
    "crates/yadb-db/src/lib.rs:940": ["acquire_writer_if_legacy"],
    "crates/yadb-db/src/tablet.rs:916": ["acquire_writer_if_legacy"],
    "crates/yadb-db/src/tablet.rs:982": ["acquire_writer_if_legacy"],
    "crates/yadb-block/src/lib.rs:311": ["acquire_writer_if_legacy"],
    # `docs/multiraft-skip-closure-plan.md`: the mechanisms the record reuses.
    # The refusal constant it removed is gone, so it is named by symbol only.
    # The reader-lease gate and the `begin_read_lease` / `read_part` citations
    # were dropped when that section was rewritten: the implementation is on
    # `feat/reader-lease`, so a line number taken from `main` would describe the
    # tree before the change. Those symbols are named in prose instead.
    "crates/yadbd/src/gc.rs:634": ["sweep_groups"],
    "crates/yadb-blob/src/pool.rs:1600": ["repair_blob"],
    "crates/yadb-blob/src/volume.rs:634": ["tombstones"],
    "crates/yadb-metakv/src/openraft_client.rs:157": ["group_id"],
    "crates/yadb-metakv/src/raft.rs:378": ["try_linearizable_group_snapshot"],
    "crates/yadb-s3/src/lib.rs:21": ["INTENT_GRACE_SECS"],
    # `collect_orphans_in_scopes_with`, which the prefix-scoped repair pass
    # deliberately does not copy. It no longer refuses a `project_ids == None`
    # scope: the project set is derived from the group's own fenced snapshot.
    "crates/yadb-s3/src/lib.rs:1619": ["collect_orphans_in_scopes_with"],
}

# Citations inside explicitly dated snapshots. Only the file and range are
# checked: these line numbers describe a tree that has since changed, and the
# surrounding section says so.
HISTORICAL_ANCHORS = {
    "crates/yadb-metakv/src/openraft_multigroup_test.rs:245",
    "crates/yadb-metakv/src/openraft_process_test.rs:151",
    "crates/yadb-metakv/src/openraft_state_machine.rs:723",
    "crates/yadb-metakv/src/raft.rs:4-21",
}


def iter_citations(docs=DOCS):
    """Yield (doc, path, start, end) for every repo-relative citation."""
    for doc in sorted(Path(docs).glob("*.md")):
        for match in CITATION_RE.finditer(doc.read_text()):
            start = int(match.group(2))
            end = int(match.group(3)) if match.group(3) else start
            yield doc, match.group(1), start, end


def citation_key(path, start, end):
    return f"{path}:{start}" if start == end else f"{path}:{start}-{end}"


def resolve(path, start, end):
    """Return the cited lines, or raise AssertionError explaining why not."""
    target = (REPO_ROOT / path).resolve()
    try:
        target.relative_to(REPO_ROOT)
    except ValueError:
        raise AssertionError(f"{path}: citation escapes the repository") from None
    if not target.is_file():
        raise AssertionError(f"{path}: cited file does not exist")
    lines = target.read_text().splitlines()
    if start < 1 or end < start:
        raise AssertionError(f"{path}:{start}-{end}: impossible range")
    if len(lines) < end:
        raise AssertionError(
            f"{path}:{start}-{end}: file has only {len(lines)} lines")
    return lines[start - 1:end]


def check_citation(path, start, end, claims=CURRENT_CLAIMS):
    """Check one citation; `claims` is injectable so tests can invert it."""
    cited = resolve(path, start, end)
    key = citation_key(path, start, end)
    if key not in claims:
        raise AssertionError(
            f"{key}: no required substrings recorded, so a moved line could "
            f"pass; register it as a current claim or a historical anchor")
    text = "\n".join(cited)
    for needle in claims[key]:
        if needle not in text:
            raise AssertionError(
                f"{key}: cited line no longer contains {needle!r}: "
                f"{text.strip()!r}")
    return text


class DocAnchorTest(unittest.TestCase):
    """Every repo-relative docs/ citation resolves, and current claims hold."""

    def test_every_citation_is_registered_as_current_or_historical(self):
        unregistered = sorted(
            {citation_key(path, start, end)
             for _, path, start, end in iter_citations()}
            - set(CURRENT_CLAIMS) - HISTORICAL_ANCHORS)
        self.assertEqual(
            unregistered, [],
            "docs cite repo paths that are registered in neither CURRENT_CLAIMS "
            "nor HISTORICAL_ANCHORS, so nothing would notice them rotting")

    def test_every_registered_citation_is_still_present_in_the_docs(self):
        cited = {citation_key(path, start, end)
                 for _, path, start, end in iter_citations()}
        missing = sorted((set(CURRENT_CLAIMS) | HISTORICAL_ANCHORS) - cited)
        self.assertEqual(
            missing, [],
            "these citations are registered but no doc cites them any more")

    def test_current_claims_still_contain_the_code_they_describe(self):
        for key in CURRENT_CLAIMS:
            path, _, line = key.rpartition(":")
            with self.subTest(citation=key):
                check_citation(path, int(line), int(line))

    def test_historical_anchors_resolve_to_a_real_line_range(self):
        for key in HISTORICAL_ANCHORS:
            path, _, spec = key.rpartition(":")
            start, _, end = spec.partition("-")
            with self.subTest(citation=key):
                lines = resolve(path, int(start), int(end or start))
                self.assertTrue(lines, "a historical range must not be empty")

    def test_an_unregistered_citation_is_rejected(self):
        # The guard must not be vacuous: a citation with no recorded expectation
        # is refused rather than trusted.
        with self.assertRaisesRegex(AssertionError, "no required substrings"):
            check_citation("crates/yadb-metakv/src/gc.rs", 790, 790, claims={})

    def test_a_moved_line_is_rejected(self):
        # Same path and line, wrong code: the substring table must notice even
        # though the line exists and is non-empty.
        with self.assertRaisesRegex(AssertionError, "no longer contains"):
            check_citation(
                "crates/yadb-metakv/src/gc.rs", 790, 790,
                claims={"crates/yadb-metakv/src/gc.rs:790": ["definitely_gone"]})

    def test_a_range_beyond_the_file_end_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "file has only"):
            resolve("crates/yadb-metakv/src/raft.rs", 4, 100_000)

    def test_a_missing_file_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "does not exist"):
            resolve("crates/yadb-metakv/src/no-such-file.rs", 1, 1)

    def test_a_real_claim_passes(self):
        self.assertIn(
            "acquire_writer",
            check_citation("crates/yadb-metakv/src/gc.rs", 790, 790))


if __name__ == "__main__":
    unittest.main()
