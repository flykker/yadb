#!/usr/bin/env python3
"""Guard: the adapted OpenRaft storage suite is mapped case by case.

`crates/yadb-metakv/src/openraft_suite.rs` is an adapted copy of the
`openraft-0.9.25` storage suite. It cannot run the unmodified upstream case
bodies: the upstream harness requires `NodeId: From<u64>` (YADB's node id is
`u32`), constructs `LogFlushed` with a crate-private constructor, and reads
`RaftState` accessors that are crate-private. The adaptation is therefore the
second branch of the TODO item: equivalent coverage, documented case by case.

This guard makes that mapping machine-checked. It fails when:

(i)   upstream runs a case that has no mapping entry;
(ii)  a mapped adapted function no longer exists;
(iii) the upstream case set under the vendored `openraft-0.9.25` changed;
(iv)  a mapping entry is stale (no upstream case, or the adapted `test_store`
      no longer runs it);
(v)   an adapted case whose assertions are meant to be unchanged loses or
      alters one (a silent weakening), or a view-substitution case's assertion
      digest no longer matches the reviewed digest;
(vi)  a split case added for an upstream case stops carrying the upstream
      assertions it was created for, or the mapped cases are reordered.

Upstream is read from the cargo registry; it is never vendored into the repo.
Set `CARGO_HOME` when the registry is not under `~/.cargo`.

Run directly: `python3 scripts/test_openraft_suite_mapping.py`.
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
ADAPTED = ROOT / "crates" / "yadb-metakv" / "src" / "openraft_suite.rs"
LOCKFILE = ROOT / "Cargo.lock"

UPSTREAM_VERSION = "0.9.25"
UPSTREAM_REL = pathlib.Path("src") / "testing" / "suite.rs"

# The authoritative upstream case order, extracted from
# `Suite::test_store` in openraft-0.9.25. The guard compares the parsed set
# against this list, so a registry change or a different version fails.
UPSTREAM_CASES = [
    "last_membership_in_log_initial",
    "last_membership_in_log",
    "last_membership_in_log_multi_step",
    "get_membership_initial",
    "get_membership_from_log_and_empty_sm",
    "get_membership_from_empty_log_and_sm",
    "get_membership_from_log_le_sm_last_applied",
    "get_membership_from_log_gt_sm_last_applied_1",
    "get_membership_from_log_gt_sm_last_applied_2",
    "get_initial_state_without_init",
    "get_initial_state_membership_from_log_and_sm",
    "get_initial_state_with_state",
    "get_initial_state_last_log_gt_sm",
    "get_initial_state_last_log_lt_sm",
    "get_initial_state_log_ids",
    "get_initial_state_re_apply_committed",
    "save_vote",
    "get_log_entries",
    "limited_get_log_entries",
    "try_get_log_entry",
    "initial_logs",
    "get_log_state",
    "get_log_id",
    "last_id_in_log",
    "last_applied_state",
    "purge_logs_upto_0",
    "purge_logs_upto_5",
    "purge_logs_upto_20",
    "delete_logs_since_11",
    "delete_logs_since_0",
    "append_to_log",
    "snapshot_meta",
    "apply_single",
    "apply_multiple",
    "transfer_snapshot",
]

# upstream case -> mapping entry.
#
# status:
#   "same"                assertions are identical after token normalisation;
#                         only formatting and the node-id spelling differ.
#   "input-substitution"  assertions are identical; an input sequence differs
#                         because the upstream sequence violates a durable
#                         store's contract.
#   "view-substitution"   assertions observe the same facts through public
#                         APIs; the exact upstream field is crate-private.
#   "weaker"              the adapted body asserts a strict subset of the
#                         upstream assertions; `dropped` names each one.
#
# `weaker` is non-empty whenever an upstream claim is not asserted verbatim,
# including view substitutions whose target is not observable. `dropped` is
# used by the guard to prove a "weaker" body dropped only the listed claims.
MAPPING = {
    "last_membership_in_log_initial": {
        "adapted": "last_membership_in_log_initial",
        "status": "same",
        "note": "formatting only",
    },
    "last_membership_in_log": {
        "adapted": "last_membership_in_log",
        "status": "same",
        "note": "formatting only",
    },
    "last_membership_in_log_multi_step": {
        "adapted": "last_membership_in_log_multi_step",
        "status": "same",
        "note": "formatting only",
    },
    "get_membership_initial": {
        "adapted": "get_membership_initial",
        "status": "same",
        "note": "formatting only",
    },
    "get_membership_from_log_and_empty_sm": {
        "adapted": "get_membership_from_log_and_empty_sm",
        "status": "same",
        "note": "formatting only",
    },
    "get_membership_from_empty_log_and_sm": {
        "adapted": "get_membership_from_empty_log_and_sm",
        "status": "same",
        "note": "formatting only",
    },
    "get_membership_from_log_le_sm_last_applied": {
        "adapted": "get_membership_from_log_le_sm_last_applied",
        "status": "same",
        "note": "formatting only",
    },
    "get_membership_from_log_gt_sm_last_applied_1": {
        "adapted": "get_membership_from_log_gt_sm_last_applied_1",
        "status": "same",
        "note": "formatting only",
    },
    "get_membership_from_log_gt_sm_last_applied_2": {
        "adapted": "get_membership_from_log_gt_sm_last_applied_2",
        "status": "same",
        "note": "formatting only",
    },
    "get_initial_state_without_init": {
        "adapted": "get_initial_state_without_init",
        "status": "view-substitution",
        "note": (
            "upstream compares the whole RaftState with a crate-private vote "
            "timestamp patched; the adapted case asserts every public field "
            "(vote, committed, server_state, membership_state, snapshot_meta) "
            "plus the store's empty log state"
        ),
        "weaker": (
            "the private RaftState fields (io_state, accepted, purge_upto, "
            "purged_next) and the internal log-id cache are not asserted; "
            "the public fields and the store's log state are"
        ),
    },
    "get_initial_state_membership_from_log_and_sm": {
        "adapted": "get_initial_state_membership_from_log_and_sm",
        "status": "weaker",
        "note": (
            "upstream's first section (empty log, membership from the state "
            "machine) is executed as get_initial_state_membership_empty_log on "
            "a fresh store. Its recovery call purges through index 2; the next "
            "upstream section appends index 1, and the durable store rejects "
            "that with `cannot append purged logs`. The append contract "
            "forbids a hole below the purge boundary, so the upstream "
            "sequence cannot hold for a durable file-backed store"
        ),
        "weaker": (
            "this body no longer asserts section 1 itself; the assertion is "
            "covered by the extra case, which uses the same input and asserts "
            "the same membership"
        ),
        "dropped": [
            "assert_eq ! ( & Membership : : new ( vec ! [ btreeset ! { 3 , 4 , 5 } ] , None ) , initial . membership_state . effective ( ) . membership ( ) )",
        ],
    },
    "get_initial_state_with_state": {
        "adapted": "get_initial_state_with_state",
        "status": "view-substitution",
        "note": (
            "`initial.committed` is public and asserted exactly; the reported "
            "last_log_id is crate-private and is read from the log store, "
            "which recovery does not mutate in this case"
        ),
        "weaker": (
            "the RaftState's cached last_log_id is not asserted; the store's "
            "last_log_id is asserted instead (the same value here)"
        ),
    },
    "get_initial_state_last_log_gt_sm": {
        "adapted": "get_initial_state_last_log_gt_sm",
        "status": "view-substitution",
        "note": (
            "the reported last_log_id is crate-private and is read from the "
            "log store, which recovery does not mutate in this case"
        ),
        "weaker": (
            "the RaftState's cached last_log_id is not asserted; the store's "
            "last_log_id is asserted instead (the same value here)"
        ),
    },
    "get_initial_state_last_log_lt_sm": {
        "adapted": "get_initial_state_last_log_lt_sm",
        "status": "view-substitution",
        "note": (
            "upstream asserts the RaftState's reported last_log_id after "
            "recovery purges through the state-machine's applied log; that "
            "cache is crate-private. The adapted case asserts the public "
            "`committed` field (which recovery sets to the same applied log), "
            "the purge marker, and the suffix the store retains"
        ),
        "weaker": (
            "the private cached last_log_id accessor is not exercised. The "
            "public `committed` field holds the same value here, so the case "
            "still pins (3,1), but it is a different field. The store keeps "
            "the entry at index 2, so the case also asserts (1,2) as the "
            "retained suffix and (3,1) as the purge marker"
        ),
    },
    "get_initial_state_log_ids": {
        "adapted": "get_initial_state_log_ids",
        "status": "view-substitution",
        "note": (
            "the internal LogIdList type and its key_log_ids method are "
            "crate-private; the adapted case rebuilds the same key-log-id view "
            "from the public log API (purge marker plus the first entry of "
            "each leader term)"
        ),
        "weaker": (
            "the assertion no longer exercises the LogIdList cache loaded by "
            "get_initial_state; it asserts the store data that cache is built "
            "from. The expected values are unchanged"
        ),
    },
    "get_initial_state_re_apply_committed": {
        "adapted": "get_initial_state_re_apply_committed",
        "status": "weaker",
        "note": (
            "upstream asserts both RaftState::io_applied and the state "
            "machine's applied log; io_applied is crate-private"
        ),
        "weaker": (
            "the RaftState::io_applied assertion is dropped. The state "
            "machine's applied log is asserted, which is the observable effect "
            "of re-applying the committed logs"
        ),
        "dropped": [
            "assert_eq ! ( Some ( & log_id_0 ( 1 , 4 ) ) , initial . io_applied ( ) , \"last_applied is updated\" )",
        ],
    },
    "save_vote": {
        "adapted": "save_vote",
        "status": "same",
        "note": "node id 0 spelled through SuiteNodeId",
    },
    "get_log_entries": {
        "adapted": "get_log_entries",
        "status": "same",
        "note": "formatting only",
    },
    "limited_get_log_entries": {
        "adapted": "limited_get_log_entries",
        "status": "same",
        "note": "formatting only",
    },
    "try_get_log_entry": {
        "adapted": "try_get_log_entry",
        "status": "same",
        "note": "formatting only",
    },
    "initial_logs": {
        "adapted": "initial_logs",
        "status": "same",
        "note": "formatting only",
    },
    "get_log_state": {
        "adapted": "get_log_state",
        "status": "same",
        "note": "formatting only",
    },
    "get_log_id": {
        "adapted": "get_log_id",
        "status": "same",
        "note": "formatting only",
    },
    "last_id_in_log": {
        "adapted": "last_id_in_log",
        "status": "same",
        "note": "formatting only",
    },
    "last_applied_state": {
        "adapted": "last_applied_state",
        "status": "input-substitution",
        "note": (
            "upstream applies (1,5) directly after (1,3). The durable state "
            "machine rejects that with `state-machine apply is not "
            "consecutive`, because Raft only applies consecutive committed "
            "entries, so the adapted case inserts the blank (1,4). Assertions "
            "are unchanged"
        ),
    },
    "purge_logs_upto_0": {
        "adapted": "purge_logs_upto_0",
        "status": "same",
        "note": "formatting only",
    },
    "purge_logs_upto_5": {
        "adapted": "purge_logs_upto_5",
        "status": "same",
        "note": "formatting only",
    },
    "purge_logs_upto_20": {
        "adapted": "purge_logs_upto_20",
        "status": "same",
        "note": "formatting only",
    },
    "delete_logs_since_11": {
        "adapted": "delete_logs_since_11",
        "status": "same",
        "note": "formatting only",
    },
    "delete_logs_since_0": {
        "adapted": "delete_logs_since_0",
        "status": "same",
        "note": "formatting only",
    },
    "append_to_log": {
        "adapted": "append_to_log",
        "status": "same",
        "note": "formatting only",
    },
    "snapshot_meta": {
        "adapted": "snapshot_meta",
        "status": "same",
        "note": "formatting only",
    },
    "apply_single": {
        "adapted": "apply_single",
        "status": "same",
        "note": "formatting only",
    },
    "apply_multiple": {
        "adapted": "apply_multiple",
        "status": "same",
        "note": "formatting only",
    },
    "transfer_snapshot": {
        "adapted": "transfer_snapshot",
        "status": "same",
        "note": "formatting only",
    },
}

# Adapted cases with no upstream counterpart. Each must still be run by
# `test_store`, and `assertions` must be exactly the upstream claims it
# carries, so a split case cannot quietly lose its coverage.
EXTRA_CASES = {
    "get_initial_state_membership_empty_log": {
        "reason": (
            "covers the first section of upstream "
            "get_initial_state_membership_from_log_and_sm on a fresh store; "
            "running it in the shared body would purge through index 2 before "
            "the next section appends index 1, which the durable store rejects"
        ),
        "covers": "get_initial_state_membership_from_log_and_sm",
        "assertions": [
            "assert_eq ! ( & Membership : : new ( vec ! [ btreeset ! { 3 , 4 , 5 } ] , None ) , initial . membership_state . effective ( ) . membership ( ) )",
        ],
    },
}

# Reviewed assertion digests for view substitutions: (upstream_count,
# upstream_sha256, adapted_count, adapted_sha256). A change on either side
# fails until this table is re-reviewed.
VIEW_DIGESTS = {
    "get_initial_state_without_init": (
        1,
        "0019d8ff39da98f61432ef9ce1f8346c490cb759aa07f8c31434cbffd9dbd3e8",
        7,
        "806dbe685dd022c5f8e0b5dd5b488cbe0b382ccfde6ebd995408122d2332b1a1",
    ),
    "get_initial_state_with_state": (
        3,
        "21e834890eeb8902636ff42ae34878dd76e7606ec77fff8635aa515b03803534",
        3,
        "f90ed58e126955fe0894944b17ccc1c2d62a7c173cb66b47635ed59b7b0c21a1",
    ),
    "get_initial_state_last_log_gt_sm": (
        1,
        "71a025577770a152d83f33c509189afea70d3fde5aaadb25a94946009fd85a18",
        1,
        "517a27502fc11129178cbd839506f9d1750b7b98bd5a429799e1082c44c5f823",
    ),
    "get_initial_state_last_log_lt_sm": (
        2,
        "7f40cd7f042a97a098813c81e5c5b4b131e146280f831579786fe8ac5c471804",
        3,
        "369a7d636c628390bd0d54fc74b9467f6f2db0425f0f4d226d31e299f26c5d77",
    ),
    "get_initial_state_log_ids": (
        9,
        "2546b974a7bfdc33f75c87b4241dcdc4fd4b1e3e5f460998f9a7d06bb418c4a5",
        9,
        "b076578fdaffc09572fa8fa5af96156dfd469a8ca5e7a6938034afb60ab2e812",
    ),
}


def find_upstream() -> pathlib.Path:
    """Locate the vendored openraft suite, without copying it into the repo."""
    cargo_home = pathlib.Path(os.environ.get("CARGO_HOME", pathlib.Path.home() / ".cargo"))
    pattern = f"registry/src/*/openraft-{UPSTREAM_VERSION}/{UPSTREAM_REL.as_posix()}"
    matches = sorted(cargo_home.glob(pattern))
    if not matches:
        raise SystemExit(
            f"openraft {UPSTREAM_VERSION} source not found under {cargo_home} "
            f"({pattern}); fetch the dependency or set CARGO_HOME"
        )
    if len(matches) > 1:
        raise SystemExit(
            f"ambiguous openraft {UPSTREAM_VERSION} source: "
            + ", ".join(str(m) for m in matches)
        )
    return matches[0]


def strip_comments(src: str) -> str:
    """Remove Rust comments while leaving string literals intact."""
    out = []
    i = 0
    state = "code"
    while i < len(src):
        c = src[i]
        if state == "code":
            if c == "/" and i + 1 < len(src) and src[i + 1] == "/":
                state = "line"
                i += 2
                out.append(" ")
            elif c == "/" and i + 1 < len(src) and src[i + 1] == "*":
                state = "block"
                i += 2
                out.append(" ")
            elif c == '"':
                state = "str"
                out.append(c)
                i += 1
            else:
                out.append(c)
                i += 1
        elif state == "line":
            if c == "\n":
                state = "code"
                out.append("\n")
            i += 1
        elif state == "block":
            if c == "*" and i + 1 < len(src) and src[i + 1] == "/":
                state = "code"
                i += 2
            else:
                i += 1
        else:  # string literal
            out.append(c)
            if c == "\\" and i + 1 < len(src):
                out.append(src[i + 1])
                i += 2
                continue
            if c == '"':
                state = "code"
            i += 1
    return "".join(out)


def functions(src: str) -> dict[str, str]:
    """Map each impl-level function name to its brace-balanced body."""
    src = strip_comments(src)
    result = {}
    for match in re.finditer(r"\n    (?:pub )?(?:async )?fn (\w+)", src):
        name = match.group(1)
        start = src.index("{", match.end())
        depth = 0
        i = start
        while i < len(src):
            if src[i] == "{":
                depth += 1
            elif src[i] == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        result[name] = src[start:i + 1]
    return result


def parse_upstream_cases(src: str) -> list[str]:
    """Extract the case order from `Suite::test_store`."""
    body = functions(src)["test_store"]
    cases = []
    for match in re.finditer(r"run_test\(\s*builder\s*,\s*Self::(\w+)\s*\)", body):
        cases.append(match.group(1))
    for match in re.finditer(r"run_fut\(\s*Self::(\w+)\(builder\)\s*\)", body):
        cases.append(match.group(1))
    return cases


def parse_adapted_cases(src: str) -> list[tuple[str, str]]:
    """Extract (case name, function) pairs from the adapted `test_store`."""
    body = functions(src)["test_store"]
    pairs = []
    pattern = re.compile(
        r"run_case\(\s*builder\s*,\s*\"(\w+)\"\s*,\s*Self::(\w+)\s*,?\s*\)"
    )
    for match in pattern.finditer(body):
        pairs.append((match.group(1), match.group(2)))
    for match in re.finditer(r"run_fut\(\s*Self::(\w+)\(builder\)\s*\)", body):
        pairs.append((match.group(1), match.group(1)))
    return pairs


def tokenize(text: str) -> list[str]:
    tokens = []
    i = 0
    while i < len(text):
        c = text[i]
        if c.isspace():
            i += 1
            continue
        if c == '"':
            j = i + 1
            while j < len(text):
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                j += 1
            tokens.append(text[i:j])
            i = j
            continue
        if c.isalnum() or c == "_":
            j = i
            while j < len(text) and (text[j].isalnum() or text[j] == "_"):
                j += 1
            tokens.append(text[i:j])
            i = j
            continue
        tokens.append(c)
        i += 1
    return tokens


def normalize(tokens: list[str]) -> str:
    """Canonical assertion text: no whitespace, no trailing commas, node id 0."""
    out = []
    for i, token in enumerate(tokens):
        if token == "," and i + 1 < len(tokens) and tokens[i + 1] in ")]}":
            continue
        out.append(token)

    def replace(seq, replacement):
        nonlocal out
        result = []
        i = 0
        while i < len(out):
            if out[i:i + len(seq)] == seq:
                result.append(replacement)
                i += len(seq)
            else:
                result.append(out[i])
                i += 1
        out = result

    replace(["NODE_ID", ".", "into", "(", ")"], "NODE0")
    replace(["C", ":", ":", "NodeId", ":", ":", "zero", "(", ")"], "NODE0")
    return " ".join(out)


def assertions(body: str) -> list[str]:
    """Every assert!/assert_eq!/assert_ne! invocation, normalised."""
    result = []
    for match in re.finditer(r"\b(assert(?:_eq|_ne)?)!\s*([\(\[])", body):
        opener = match.group(2)
        closer = ")" if opener == "(" else "]"
        i = match.end() - 1
        depth = 0
        in_string = False
        while i < len(body):
            c = body[i]
            if in_string:
                if c == "\\":
                    i += 2
                    continue
                if c == '"':
                    in_string = False
            else:
                if c == '"':
                    in_string = True
                elif c == opener:
                    depth += 1
                elif c == closer:
                    depth -= 1
                    if depth == 0:
                        break
            i += 1
        result.append(normalize(tokenize(body[match.start():i + 1])))
    return result


def digest(items: list[str]) -> str:
    joined = "\n".join(items).encode()
    return hashlib.sha256(joined).hexdigest()


def check_static(
    upstream_cases: list[str],
    adapted_calls: list[tuple[str, str]],
    adapted_functions: set[str],
    mapping: dict,
    extra_cases: dict,
    pinned_cases: list[str],
) -> list[str]:
    """The structural checks, independent of assertion bodies."""
    errors = []

    if upstream_cases != pinned_cases:
        missing = [c for c in pinned_cases if c not in upstream_cases]
        added = [c for c in upstream_cases if c not in pinned_cases]
        if missing or added:
            errors.append(
                "upstream case set changed under the vendored version "
                f"(missing={missing}, added={added})"
            )
        else:
            errors.append(
                "upstream case order changed under the vendored version"
            )

    for case in upstream_cases:
        if case not in mapping:
            errors.append(f"{case}: upstream case has no mapping entry")

    for case in mapping:
        if case not in upstream_cases:
            errors.append(f"{case}: mapping entry has no upstream case")

    called = [case for case, _ in adapted_calls]
    for case in upstream_cases:
        if case in mapping and case not in called:
            errors.append(
                f"{case}: mapping entry is stale, adapted test_store does not run it"
            )
    for case in called:
        if called.count(case) > 1:
            errors.append(f"{case}: adapted test_store runs it more than once")
    for case, function in adapted_calls:
        if case not in mapping and case not in extra_cases:
            errors.append(f"{case}: adapted test_store runs an unmapped case")
        if case in mapping and mapping[case]["adapted"] != function:
            errors.append(
                f"{case}: mapped function is {mapping[case]['adapted']}, "
                f"but test_store calls {function}"
            )
        if function not in adapted_functions:
            errors.append(f"{case}: adapted function {function} does not exist")
    for case in extra_cases:
        if case not in called:
            errors.append(f"{case}: extra case is not run by test_store")
        if case in upstream_cases:
            errors.append(f"{case}: extra case shadows an upstream case")

    mapped_order = [case for case, _ in adapted_calls if case in mapping]
    if mapped_order != [c for c in upstream_cases if c in mapping]:
        errors.append(
            "adapted test_store runs the mapped cases in a different order "
            "than upstream; each case gets a fresh store, so this is a "
            "mapping-review signal, not a behavioural difference"
        )

    by_function = {}
    for case, function in adapted_calls:
        by_function.setdefault(function, []).append(case)
    for function, cases in by_function.items():
        if len(cases) > 1:
            errors.append(
                f"{function}: adapted function is called for more than one "
                f"case {cases}, so the mapping cannot be checked per case"
            )
    return errors


def check_assertions(
    upstream_functions: dict[str, str],
    adapted_functions: dict[str, str],
    mapping: dict,
    view_digests: dict,
    extra_cases: dict | None = None,
) -> list[str]:
    """Prove unchanged cases did not drop an assertion, and view cases match."""
    errors = []
    for case, entry in mapping.items():
        if case not in upstream_functions or entry["adapted"] not in adapted_functions:
            continue
        upstream = assertions(upstream_functions[case])
        adapted = assertions(adapted_functions[entry["adapted"]])
        status = entry["status"]

        if status in ("same", "input-substitution"):
            if adapted != upstream:
                lost = [a for a in upstream if a not in adapted]
                gained = [a for a in adapted if a not in upstream]
                errors.append(
                    f"{case}: assertions changed for a {status} case "
                    f"(lost={lost[:3]}, gained={gained[:3]})"
                )
        elif status == "weaker":
            dropped = entry.get("dropped", [])
            if not dropped:
                errors.append(f"{case}: weaker case does not list dropped assertions")
            for claim in dropped:
                if claim not in upstream:
                    errors.append(
                        f"{case}: dropped assertion is not in upstream any more: {claim}"
                    )
            expected = list(upstream)
            for claim in dropped:
                if claim in expected:
                    expected.remove(claim)
            if adapted != expected:
                lost = [a for a in expected if a not in adapted]
                gained = [a for a in adapted if a not in expected]
                errors.append(
                    f"{case}: weaker case changed beyond its listed drops "
                    f"(lost={lost[:3]}, gained={gained[:3]})"
                )
            if not entry.get("weaker"):
                errors.append(f"{case}: weaker case has no weaker explanation")
        elif status == "view-substitution":
            if not entry.get("weaker"):
                errors.append(
                    f"{case}: view substitution does not state what is weaker"
                )
            expected = view_digests.get(case)
            if expected is None:
                errors.append(f"{case}: view substitution has no reviewed digest")
            else:
                want = (
                    len(upstream),
                    digest(upstream),
                    len(adapted),
                    digest(adapted),
                )
                if want != expected:
                    errors.append(
                        f"{case}: assertion digest changed; re-review the "
                        f"substitution and update VIEW_DIGESTS "
                        f"(expected={expected}, got={want})"
                    )
        else:
            errors.append(f"{case}: unknown status {status!r}")

    for case, entry in (extra_cases or {}).items():
        if case not in adapted_functions:
            continue
        got = assertions(adapted_functions[case])
        want = entry.get("assertions", [])
        if got != want:
            errors.append(
                f"{case}: extra case assertions changed "
                f"(expected={want}, got={got})"
            )
        covers = entry.get("covers")
        if covers not in upstream_functions:
            errors.append(f"{case}: covers {covers!r}, which is not an upstream case")
            continue
        upstream = assertions(upstream_functions[covers])
        for claim in want:
            if claim not in upstream:
                errors.append(
                    f"{case}: carries an assertion that upstream {covers} "
                    f"does not make: {claim}"
                )
    return errors


def check_lockfile(text: str) -> list[str]:
    """The lockfile must pin the same openraft version the mapping was read from."""
    errors = []
    for match in re.finditer(
        r'name = "openraft"\nversion = "([^"]+)"', text
    ):
        if match.group(1) != UPSTREAM_VERSION:
            errors.append(
                f"Cargo.lock pins openraft {match.group(1)}, "
                f"but the mapping covers {UPSTREAM_VERSION}"
            )
    if not re.search(r'name = "openraft"\nversion = "', text):
        errors.append("Cargo.lock has no openraft entry")
    return errors


def check_repo() -> list[str]:
    upstream_path = find_upstream()
    if upstream_path.parents[2].name != f"openraft-{UPSTREAM_VERSION}":
        return [
            f"resolved {upstream_path}, which is not openraft-{UPSTREAM_VERSION}"
        ]
    upstream_src = upstream_path.read_text()
    adapted_src = ADAPTED.read_text()
    upstream_functions = functions(upstream_src)
    adapted_functions = functions(adapted_src)

    errors = check_lockfile(LOCKFILE.read_text())
    errors += check_static(
        parse_upstream_cases(upstream_src),
        parse_adapted_cases(adapted_src),
        set(adapted_functions),
        MAPPING,
        EXTRA_CASES,
        UPSTREAM_CASES,
    )
    errors += check_assertions(
        upstream_functions, adapted_functions, MAPPING, VIEW_DIGESTS, EXTRA_CASES
    )
    return errors


class OpenraftSuiteMappingTest(unittest.TestCase):
    """The adapted suite is mapped case by case and the mapping cannot rot."""

    def test_repo_mapping_is_complete_and_current(self):
        errors = check_repo()
        self.assertEqual(errors, [], "\n".join(errors))

    def test_an_upstream_case_without_a_mapping_entry_is_rejected(self):
        upstream = ["alpha", "beta", "gamma"]
        calls = [("alpha", "alpha"), ("beta", "beta"), ("gamma", "gamma")]
        mapping = {
            "alpha": {"adapted": "alpha", "status": "same"},
            "beta": {"adapted": "beta", "status": "same"},
        }
        errors = check_static(upstream, calls, {"alpha", "beta", "gamma"},
                              mapping, {}, upstream)
        self.assertTrue(any("gamma" in e and "no mapping entry" in e for e in errors),
                        errors)

    def test_a_mapped_function_that_no_longer_exists_is_rejected(self):
        upstream = ["alpha"]
        calls = [("alpha", "alpha")]
        mapping = {"alpha": {"adapted": "alpha", "status": "same"}}
        errors = check_static(upstream, calls, set(), mapping, {}, upstream)
        self.assertTrue(any("does not exist" in e for e in errors), errors)

    def test_a_changed_upstream_case_set_is_rejected(self):
        upstream = ["alpha", "beta"]
        calls = [("alpha", "alpha"), ("beta", "beta")]
        mapping = {
            "alpha": {"adapted": "alpha", "status": "same"},
            "beta": {"adapted": "beta", "status": "same"},
        }
        errors = check_static(upstream, calls, {"alpha", "beta"}, mapping, {},
                              ["alpha"])
        self.assertTrue(any("upstream case set changed" in e for e in errors),
                        errors)

    def test_a_stale_mapping_entry_is_rejected(self):
        upstream = ["alpha"]
        calls = [("alpha", "alpha")]
        mapping = {
            "alpha": {"adapted": "alpha", "status": "same"},
            "gone": {"adapted": "gone", "status": "same"},
        }
        errors = check_static(upstream, calls, {"alpha", "gone"}, mapping, {},
                              upstream)
        self.assertTrue(any("gone" in e and "no upstream case" in e for e in errors),
                        errors)

    def test_a_case_no_longer_run_by_test_store_is_rejected(self):
        upstream = ["alpha"]
        calls = []
        mapping = {"alpha": {"adapted": "alpha", "status": "same"}}
        errors = check_static(upstream, calls, {"alpha"}, mapping, {}, upstream)
        self.assertTrue(any("stale" in e for e in errors), errors)

    def test_a_weakened_same_case_is_rejected(self):
        upstream_functions = {
            "alpha": "{ assert_eq!(1, 1); assert_eq!(2, 2); }",
        }
        adapted_functions = {
            "alpha": "{ assert_eq!(1, 1); }",
        }
        mapping = {"alpha": {"adapted": "alpha", "status": "same"}}
        errors = check_assertions(upstream_functions, adapted_functions,
                                  mapping, {})
        self.assertTrue(any("assertions changed" in e for e in errors), errors)

    def test_a_changed_view_substitution_is_rejected(self):
        upstream_functions = {"alpha": "{ assert_eq!(1, 1); }"}
        adapted_functions = {"alpha": "{ assert_eq!(2, 2); }"}
        mapping = {
            "alpha": {
                "adapted": "alpha",
                "status": "view-substitution",
                "weaker": "the cached value is private",
            }
        }
        reviewed = {("alpha"): (1, digest(assertions(upstream_functions["alpha"])),
                               1, digest(assertions(adapted_functions["alpha"])))}
        errors = check_assertions(upstream_functions, adapted_functions,
                                  mapping, reviewed)
        self.assertEqual(errors, [])
        adapted_functions["alpha"] = "{ assert_eq!(3, 3); }"
        errors = check_assertions(upstream_functions, adapted_functions,
                                  mapping, reviewed)
        self.assertTrue(any("digest changed" in e for e in errors), errors)

    def test_a_weaker_case_may_drop_only_the_listed_assertion(self):
        upstream_functions = {
            "alpha": "{ assert_eq!(1, 1); assert_eq!(2, 2); }",
        }
        adapted_functions = {"alpha": "{ assert_eq!(1, 1); }"}
        mapping = {
            "alpha": {
                "adapted": "alpha",
                "status": "weaker",
                "weaker": "the second claim is not observable",
                "dropped": ["assert_eq ! ( 2 , 2 )"],
            }
        }
        errors = check_assertions(upstream_functions, adapted_functions,
                                  mapping, {})
        self.assertEqual(errors, [])
        adapted_functions["alpha"] = "{ }"
        errors = check_assertions(upstream_functions, adapted_functions,
                                  mapping, {})
        self.assertTrue(any("changed beyond" in e for e in errors), errors)

    def test_a_changed_lockfile_version_is_rejected(self):
        errors = check_lockfile(
            '[[package]]\nname = "openraft"\nversion = "0.9.24"\n'
        )
        self.assertTrue(any("0.9.24" in e for e in errors), errors)

    def test_a_split_extra_case_must_keep_its_carried_assertions(self):
        upstream_functions = {"alpha": "{ assert_eq!(1, 1); assert_eq!(2, 2); }"}
        adapted_functions = {"extra": "{ assert_eq!(1, 1); }"}
        extra = {
            "extra": {
                "covers": "alpha",
                "assertions": ["assert_eq ! ( 1 , 1 )"],
            }
        }
        errors = check_assertions(upstream_functions, adapted_functions, {}, {},
                                  extra)
        self.assertEqual(errors, [])
        adapted_functions["extra"] = "{ }"
        errors = check_assertions(upstream_functions, adapted_functions, {}, {},
                                  extra)
        self.assertTrue(any("extra case assertions changed" in e for e in errors),
                        errors)

    def test_an_extra_case_cannot_invent_an_upstream_assertion(self):
        upstream_functions = {"alpha": "{ assert_eq!(1, 1); }"}
        adapted_functions = {"extra": "{ assert_eq!(9, 9); }"}
        extra = {
            "extra": {
                "covers": "alpha",
                "assertions": ["assert_eq ! ( 9 , 9 )"],
            }
        }
        errors = check_assertions(upstream_functions, adapted_functions, {}, {},
                                  extra)
        self.assertTrue(
            any("does not make" in e for e in errors), errors)

    def test_a_reordered_mapped_case_is_reported(self):
        upstream = ["alpha", "beta"]
        calls = [("beta", "beta"), ("alpha", "alpha")]
        mapping = {
            "alpha": {"adapted": "alpha", "status": "same"},
            "beta": {"adapted": "beta", "status": "same"},
        }
        errors = check_static(upstream, calls, {"alpha", "beta"}, mapping, {},
                              upstream)
        self.assertTrue(any("different order" in e for e in errors), errors)

    def test_one_adapted_function_cannot_stand_for_two_cases(self):
        upstream = ["alpha", "beta"]
        calls = [("alpha", "shared"), ("beta", "shared")]
        mapping = {
            "alpha": {"adapted": "shared", "status": "same"},
            "beta": {"adapted": "shared", "status": "same"},
        }
        errors = check_static(upstream, calls, {"shared"}, mapping, {}, upstream)
        self.assertTrue(
            any("more than one" in e for e in errors), errors)


if __name__ == "__main__":
    unittest.main()
