# Working preferences

- Use GPT-6.1 Sol for orchestration and review.
- Delegate coding and implementation tasks to GPT-6.1 Terra as a subagent to
  reduce cost. Preserve existing user changes and validate delegated changes.
- Perform the final independent GPT-6.1 Sol review after implementation; do not
  represent interim checks or the orchestrator's own inspection as that review.

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
