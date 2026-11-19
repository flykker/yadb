# Monitoring configuration

Everything here consumes `GET /metrics` on the daemon's public listener
(`--listen`, default `127.0.0.1:8420`). The endpoint is a read-only projection of
the same rows `/v1/pools/health` serves: a scrape performs the slot health probe
the health endpoint performs, and writes nothing. See
`docs/multiraft-runtime-operations.md` for the families and their caveats.

| File | What it is |
| --- | --- |
| `prometheus.yml` | Scrape config: one `yadb` job, 15 s interval, loads `yadb-alerts.yml`. |
| `yadb-alerts.yml` | Alert rules over the exposition. |
| `grafana-dashboard.json` | Dashboard: pool/slot health, the last GC pass, the last repair scan, per-group progress. |

## Before you deploy

The public listener is plain HTTP and `/metrics` is not authenticated. It
publishes node, pool, slot and metadata-group identifiers — not subjects, keys,
buckets or object names. Either scrape it from the same host, or terminate TLS
and authenticate in front of it, or bound it with a firewall that admits only
the scraper. Do not expose it to the internet.

## What the alerts mean

A **refusal** is a correct outcome, not an error. The collection pass refuses
when a live lease, a non-leader group or an unreachable pool makes deletion
unsafe, and it deletes nothing in that case. `YadbGcRefused` therefore fires at
`warning`, not `critical`, and the response is to look at
`yadb_gc_leases_live`, `yadb_gc_groups_refused` and `yadb_gc_pools_refused` to
see which fence held and whether it clears — not to force the pass.

`YadbScrubUnrecoverable` is the one that means data loss: at least one live blob
did not decode or sat on an unreachable target. Read the scan's outcome, not
only the count.

Two exposition shapes shape the rules, and both are easy to get wrong in a new
rule:

* The families describing the **last** pass are **gauges**. A smaller pass
  deletes fewer fragments, so the value goes down. Never wrap them in `rate()`
  or `increase()`; the cumulative families are the ones ending in `_total`.
* A family that reports the last pass is emitted only once a pass has run, so
  freshness is `yadb_gc_last_pass_present` / `yadb_scrub_last_pass_present`,
  which are always present and are `0` before the first pass.

## Verifying a change

```
python3 scripts/test_metrics_alert_rules.py
```

The guard parses every alert and panel expression, every `family(...)`
declaration in `crates/yadbd/src/metrics.rs` and the `FAMILIES` table, and
fails when an expression names a family the exposition does not declare, when a
rate is applied to a gauge, when a counter's name does not end in `_total` (or a
gauge's does), when the `up` alert names a job the scrape config does not
define, or when `rule_files` points at a missing file. It also unit-tests those
checks against deliberately broken input, so a guard that stops catching
anything fails itself.

The guard proves the configuration and the code agree. It does not prove the
thresholds are right for a real deployment: nobody has run these rules against
a multi-node cluster, so treat the durations and severities as starting points
and tune them against observed traffic.
