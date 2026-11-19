#!/usr/bin/env python3
"""Guard: a backticked identifier in `docs/` must exist in the tree.

`scripts/test_doc_anchors.py` checks the *line* a citation points at. This
checks the other half: the names. Every document in `docs/` names tests,
functions, counters and configuration fields in backticks, and a rename or a
removal silently turns such a name into a claim about code that is not there.
That is not a cosmetic problem: `multiraft-verification.md` listed fourteen
tests as current evidence long after `634efeb` deleted the legacy `RaftCore`
and every one of them.

The rule is deliberately narrow so it stays useful:

* only lowercase identifiers of twelve or more characters are considered, which
  is what a Rust function, a test name, a counter and a configuration field
  look like, and what prose in backticks almost never looks like;
* a name passes if it occurs anywhere under `crates/`, `tests/` or `scripts/`;
* otherwise it must be listed below with a reason. A listed name that starts
  existing again fails the test, so the list cannot rot into a blanket
  exemption.

Three lists exist, and the difference matters:

* `EXTERNAL` - the name belongs to a dependency, not to YADB. The reason names
  the crate and the file so the claim can be checked.
* `HISTORICAL` - the name described YADB code that has since been removed. The
  reason names the commit that removed it. Keeping these is the point: the
  documents narrate what a fix did, and a fix to deleted code is still worth
  narrating, as long as the text says so.
* `NOT_IDENTIFIERS` - the backticked word is not a code name at all: a
  directory, a PostgreSQL condition name, a proposed manifest field in a design
  draft, or a word that only looks like an identifier.

Nothing here decides whether a claim is true. It decides whether the name in
the claim exists, which is the part a machine can settle.
"""

from __future__ import annotations

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent

SOURCE_SUFFIXES = {".rs", ".py", ".toml"}
SOURCE_DIRS = ["crates", "tests", "scripts"]

IDENTIFIER_RE = re.compile(r"`([a-z][a-z0-9_]{11,})`")

# Names that live in a dependency. Verified against the vendored source; the
# reason is the file that defines it, so a reader can re-check it.
EXTERNAL = {
    "handle_apply_result": "openraft-0.9.25 src/core/raft_core.rs:772",
    "send_progress_error": "openraft-0.9.25 src/replication/mod.rs:518",
    "update_progress": "openraft-0.9.25 src/engine/handler/replication_handler/mod.rs:267",
    "install_snapshot_timeout": "openraft-0.9.25 src/config/config_test.rs:76",
}

# Names that described YADB code which has since been removed. The reason is
# the commit that removed it, so the document's narrative can be dated.
HISTORICAL = {
    "execute_with": "removed with the legacy RaftCore in ad67c94",
    "drive_outbound_with": "removed with the legacy RaftCore in ad67c94",
    "execute_keeps_waiting_through_silence_shorter_than_the_no_quorum_window":
        "legacy wait regression, removed with execute_with in ad67c94",
    "execute_refuses_after_sustained_silence_and_before_the_commit_budget":
        "legacy wait regression, removed with execute_with in ad67c94",
    "open_clustered": "legacy singleton bootstrap, removed in 634efeb",
    "applied_term": "legacy checkpoint field, removed in 634efeb",
    "pending_repairs": "in-memory degraded-write queue, removed in 1825f84",
    "connections_throttled": "unreachable counter, replaced in 2c73c60-era work",
    "xml_unescape": "loose completion parser, replaced in 2c73c60",
    "xml_validate_attribute_value": "loose completion parser, replaced in 2c73c60",
    "valid_char_reference": "loose completion parser, replaced in 2c73c60",
    "assert_multiraft_scrub_repair_refusal": "test helper removed in 9121448",
    # The legacy RaftCore checkpoint/compaction suite. `multiraft-verification.md`
    # keeps the names on purpose: they are listed as history next to the current
    # coverage of the same shapes, not as current evidence.
    "compaction_bounds_the_wal_and_preserves_state_across_reopen": "legacy RaftCore suite, removed in 634efeb",
    "checkpoint_renamed_before_the_wal_rewrite_is_reconciled_on_open": "legacy RaftCore suite, removed in 634efeb",
    "a_compacted_base_without_a_checkpoint_refuses_to_start": "legacy RaftCore suite, removed in 634efeb",
    "an_unparsable_checkpoint_refuses_to_start": "legacy RaftCore suite, removed in 634efeb",
    "a_checkpoint_from_another_membership_refuses_to_start": "legacy RaftCore suite, removed in 634efeb",
    "term_and_vote_survive_compaction": "legacy RaftCore suite, removed in 634efeb",
    "compaction_never_drops_an_entry_above_a_stopped_peer_match": "legacy RaftCore suite, removed in 634efeb",
    "a_peer_below_the_compacted_base_is_sent_the_durable_image": "legacy RaftCore suite, removed in 634efeb",
    "a_replica_that_lost_its_log_rejoins_through_the_durable_image": "legacy RaftCore suite, removed in 634efeb",
    "a_snapshot_from_another_group_or_membership_is_refused": "legacy RaftCore suite, removed in 634efeb",
    "a_corrupted_or_stale_snapshot_is_refused_without_publishing": "legacy RaftCore suite, removed in 634efeb",
    "a_snapshot_transfer_resumes_without_duplicating_bytes": "legacy RaftCore suite, removed in 634efeb",
    "an_install_interrupted_by_a_power_cut_leaves_the_old_state": "legacy RaftCore suite, removed in 634efeb",
    "a_crash_after_publishing_the_image_keeps_the_new_base": "legacy RaftCore suite, removed in 634efeb",
}

# Backticked words that are not code names.
NOT_IDENTIFIERS = {
    "experimental": "removed flag-name prefix, quoted as a string",
    "cluster_data": "a deployment directory name, not a symbol",
    "cannot_connect_now": "PostgreSQL condition name for SQLSTATE 57P03",
    "format_version": "proposed manifest field in the migration design draft",
    "migration_id": "proposed manifest field in the migration design draft",
    "source_image": "proposed manifest field in the migration design draft",
}


def source_corpus() -> str:
    parts = []
    own = pathlib.Path(__file__).resolve()
    for directory in SOURCE_DIRS:
        base = ROOT / directory
        if not base.is_dir():
            continue
        for path in base.rglob("*"):
            if path.suffix not in SOURCE_SUFFIXES or not path.is_file():
                continue
            # A guard must not satisfy itself: the names in this file's own
            # allowlist and in its self-test would otherwise count as evidence.
            if path.resolve() == own:
                continue
            parts.append(path.read_text(errors="ignore"))
    return "\n".join(parts)


def is_identifier(name: str) -> bool:
    # A 32+ character run of hex digits is a checksum, not a symbol.
    if len(name) >= 32 and all(c in "0123456789abcdef" for c in name):
        return False
    return True


def doc_identifiers(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for match in IDENTIFIER_RE.finditer(text):
        name = match.group(1)
        if not is_identifier(name):
            continue
        counts[name] = counts.get(name, 0) + 1
    return counts


def missing_identifiers(corpus: str, text: str) -> dict[str, int]:
    return {
        name: count
        for name, count in doc_identifiers(text).items()
        if name not in corpus
    }


def classify(missing: dict[str, int]) -> dict[str, list[str]]:
    unclassified = []
    for name in sorted(missing):
        if name in EXTERNAL or name in HISTORICAL or name in NOT_IDENTIFIERS:
            continue
        unclassified.append(name)
    return {"unclassified": unclassified}


class DocTestNameTest(unittest.TestCase):
    def setUp(self) -> None:
        self.corpus = source_corpus()
        self.assertTrue(self.corpus, "source corpus is empty; the globs are wrong")

    def test_every_doc_identifier_exists(self) -> None:
        problems: list[str] = []
        for doc in sorted((ROOT / "docs").glob("*.md")):
            missing = missing_identifiers(self.corpus, doc.read_text())
            verdict = classify(missing)
            for name in verdict["unclassified"]:
                problems.append(
                    f"{doc.name}: `{name}` is named in the document "
                    f"({missing[name]}x) and exists nowhere under "
                    f"{', '.join(SOURCE_DIRS)}; either restore it, re-point the "
                    f"claim at the name that does exist, or list it with a reason"
                )
        if problems:
            self.fail("\n".join(problems))

    def test_allowlisted_names_are_still_absent(self) -> None:
        """A name that exists again must leave the allowlist."""
        stale: list[str] = []
        for name, reason in {
            **EXTERNAL,
            **HISTORICAL,
            **NOT_IDENTIFIERS,
        }.items():
            if name in self.corpus:
                stale.append(f"{name}: {reason}")
        if stale:
            self.fail(
                "these allowlist entries now exist in the tree, so the reason "
                "is wrong and the entry must be removed:\n" + "\n".join(stale)
            )

    def test_guard_detects_a_missing_identifier(self) -> None:
        fake = "A test named `this_test_does_not_exist_anywhere` proves it."
        missing = missing_identifiers(self.corpus, fake)
        self.assertIn("this_test_does_not_exist_anywhere", missing)
        self.assertEqual(
            classify(missing)["unclassified"], ["this_test_does_not_exist_anywhere"]
        )

    def test_short_words_are_not_identifiers(self) -> None:
        # Prose in backticks must not trip the guard.
        missing = missing_identifiers(self.corpus, "`pool`, `lease`, `GET`, `409`")
        self.assertEqual(missing, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
