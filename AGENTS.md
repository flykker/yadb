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
