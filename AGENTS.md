# Working preferences

- Use GPT-6.1 Sol for orchestration and review.
- Delegate coding and implementation tasks to GPT-6.1 Terra as a subagent to
  reduce cost. Preserve existing user changes and validate delegated changes.
- Perform the final independent review after implementation; do not represent
  interim checks or the orchestrator's own inspection as that review.

## Review model

- Preferred reviewer: `plusvibe/gpt-6.1-sol:purple#high`.
- Sol has failed twice on this machine by returning an invalid tool call on
  every route (once billed as an error, once cancelled mid-run) for long
  read-only review prompts. When Sol cannot complete a review, run the same
  independent review on `plusvibe/gpt-6.1-terra:purple#high` and record which
  model produced the verdict. Do not describe an orchestrator self-check as a
  review, and do not retry Sol more than twice for the same target.
- Keep review prompts compact: state the commit range, the claims to verify,
  the read-only rule, and the output shape. Long prompts with many permitted
  commands correlate with the failed runs.

## Parallel execution

- Work on independent blocks in parallel: at most **two** worktree subagents
  (one worktree/branch each, never the same files as the orchestrator) plus one
  task the orchestrator does itself in the main checkout.
- Pick non-overlapping areas so merges stay trivial; report each branch and its
  commits separately and merge them one at a time.

## Clusters

- Start a test cluster only for a test run, and stop it immediately afterwards.
  Never leave daemons running between blocks; stop before switching tasks or
  when a run is finished, keeping the data directories for reuse.
- Prefer the fixed port bases already documented (dev 18000+, protocols.py
  19000+, legacy live 22000+, per-block bases for parallel work) so parallel
  work never collides.

## Threads for builds and tests (user directive, 2026-11-20)

The user asked to use threads to the maximum for builds and tests. This
overrides the earlier conservative `-j 2` habit.

- Machine: 12 cores, 22 GiB RAM, `/tmp` is tmpfs, `/` has 941G free.
  Disk is **not** the bottleneck: 494 MB/s direct, podman overlay 1.0 GB/s,
  tmpfs 1.3 GB/s, `/proc/pressure/io` 0.00. Do not move `target/` to tmpfs to
  fix a problem that does not exist.
- Build with `-j 12` (or cargo's default nproc), not `-j 2`. But do not run 7
  worktrees at `-j 12` at once (84 rustc on 12 cores thrashes): cap the sum,
  e.g. `-j 4` each across 3 trees, or serialise the big builds.
- `cargo test` already uses 12 test threads. Do not pass `--test-threads=1`
  except where a test genuinely needs serialisation (`yadbd --bin yadbd`
  binds fixed ports and does).
- Sharding one libtest binary into N processes works and is a real speedup:
  libtest accepts several filters after `--` and unions them (verified:
  `gc::` 21 + `multiraft::` 27 = 48); `--exact <name...>` shards explicitly.
- Measured, `yadb-metakv` (260 tests): 1 thread 114 s; 12 threads 30 s;
  6 shards x 4 threads 30.9 s; the same 6 shards **without**
  `large_snapshot_stream_round_trip` 10.6 s. That one test alone is 33 s at
  99% CPU: 17 MiB of snapshot data JSON-encoded twice (16 bytes per byte worst
  case). It is a real production cost, not a test artifact.
- Rule: shard only when no single test exceeds ~1/3 of total time; otherwise
  attack the dominant test first.
- Baselines: `yadb-metakv --lib` 260 tests ~30-37 s at 12 threads;
  `yadbd --bin yadbd` 170 tests ~1.4-2.5 s at 12 threads (45 s at 1);
  `yadb-io` 18 tests instant.
- `cargo-nextest` is not installed and not in the cargo cache, so it cannot be
  added with `--offline`; manual sharding is the only option.
