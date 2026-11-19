"""Offline checks that the shipped monitoring config matches the exposition.

`deploy/monitoring/` contains the Prometheus scrape config, the alert rules and
the Grafana dashboard for the daemon's `/metrics` endpoint. Every expression in
them names a metric family by hand, and the families live in
`crates/yadbd/src/metrics.rs`. A renamed or removed family therefore leaves a
silently dead alert behind: the rule keeps parsing, the query returns nothing,
and nobody learns that the condition it was written for is now unwatched.

This guard closes that gap without a running daemon:

* every `yadb_*` identifier in an alert or panel expression must be a family the
  exposition declares, and the declaration list must agree with the `FAMILIES`
  table the unit test already pins;
* `rate()`/`irate()`/`increase()`/`delta()` may only wrap a family declared as a
  counter. The families describing the *last* pass are gauges that legitimately
  go down, and wrapping one in `rate()` would publish a nonsense series that
  still looks plausible;
* the naming convention holds both ways: a counter's name ends in `_total`, and
  a gauge's name does not. That is the invariant behind the three
  `yadb_gc_*` families being gauges rather than `*_total` counters;
* the alert that uses Prometheus's own `up` metric names a scrape job that the
  shipped scrape config defines, and the config's `rule_files` exist.

The checks are unit-tested against deliberately broken inputs as well, so a
guard that stops catching anything fails itself.

Run directly: `python3 scripts/test_metrics_alert_rules.py`.
"""
from pathlib import Path
import json
import re
import unittest

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
METRICS_RS = REPO_ROOT / "crates" / "yadbd" / "src" / "metrics.rs"
MONITORING = REPO_ROOT / "deploy" / "monitoring"
SCRAPE_CONFIG = MONITORING / "prometheus.yml"
ALERTS = MONITORING / "yadb-alerts.yml"
DASHBOARD = MONITORING / "grafana-dashboard.json"

# `family(&mut out, "name", "kind", ...)` — the kind is the second literal.
FAMILY_RE = re.compile(
    r'family\(\s*&mut out,\s*"(?P<name>yadb_[a-z0-9_]+)",\s*"(?P<kind>gauge|counter)"'
)
FAMILIES_TABLE_RE = re.compile(r"const FAMILIES: &\[&str\] = &\[(?P<body>.*?)\n    \];", re.S)
QUOTED_RE = re.compile(r'"(yadb_[a-z0-9_]+)"')
YADB_TOKEN_RE = re.compile(r"\byadb_[a-z0-9_]+")
RATE_RE = re.compile(r"\b(?:rate|irate|increase|delta)\s*\(\s*(yadb_[a-z0-9_]+)")
UP_JOB_RE = re.compile(r'\bup\s*\{[^}]*\bjob\s*=\s*"([^"]+)"')


def declared_families() -> dict[str, str]:
    """Every family the exposition declares, mapped to its Prometheus type."""
    source = METRICS_RS.read_text()
    families: dict[str, str] = {}
    for match in FAMILY_RE.finditer(source):
        name, kind = match.group("name"), match.group("kind")
        if name in families and families[name] != kind:
            raise AssertionError(f"{name} is declared as both {families[name]} and {kind}")
        families[name] = kind
    table = FAMILIES_TABLE_RE.search(source)
    if table is None:
        raise AssertionError("metrics.rs no longer has the FAMILIES table this guard reads")
    listed = QUOTED_RE.findall(table.group("body"))
    missing = sorted(set(listed) - set(families))
    unlisted = sorted(set(families) - set(listed))
    if missing or unlisted:
        raise AssertionError(
            "the FAMILIES table and the family(...) declarations disagree: "
            f"declared but not listed {unlisted}, listed but not declared {missing}"
        )
    return families


def alert_expressions() -> list[tuple[str, str]]:
    document = yaml.safe_load(ALERTS.read_text())
    exprs = []
    for group in document["groups"]:
        for rule in group["rules"]:
            exprs.append((f"{group['name']}/{rule['alert']}", rule["expr"]))
    return exprs


def panel_expressions() -> list[tuple[str, str]]:
    document = json.loads(DASHBOARD.read_text())
    exprs = []
    for panel in document["panels"]:
        for target in panel.get("targets", []):
            exprs.append((f"{panel['title']}/{target.get('refId', '?')}", target["expr"]))
    return exprs


def check_expr(where: str, expr: str, families: dict[str, str]) -> None:
    """Fail if `expr` names an unknown family or rate()s a gauge."""
    for name in YADB_TOKEN_RE.findall(expr):
        if name not in families:
            raise AssertionError(
                f"{where}: expression {expr!r} names {name}, which the exposition "
                "does not declare (renamed or removed family?)"
            )
    for name in RATE_RE.findall(expr):
        kind = families.get(name)
        if kind != "counter":
            raise AssertionError(
                f"{where}: expression {expr!r} applies a rate to {name}, which is "
                f"declared as a {kind}. A per-pass gauge goes down by design; a "
                "rate over it is a nonsense series that still looks plausible."
            )


def check_names(families: dict[str, str]) -> None:
    """A counter's name ends in `_total`; a gauge's does not."""
    for name, kind in families.items():
        if kind == "counter" and not name.endswith("_total"):
            raise AssertionError(f"{name} is declared a counter but its name does not end in _total")
        if kind == "gauge" and name.endswith("_total"):
            raise AssertionError(f"{name} is declared a gauge but its name ends in _total")


def scrape_jobs() -> list[str]:
    document = yaml.safe_load(SCRAPE_CONFIG.read_text())
    return [job["job_name"] for job in document["scrape_configs"]]


class MonitoringConfigTest(unittest.TestCase):
    def test_every_alert_and_panel_references_a_declared_family(self):
        families = declared_families()
        exprs = alert_expressions() + panel_expressions()
        self.assertTrue(exprs, "no expressions found; the guard would pass vacuously")
        for where, expr in exprs:
            check_expr(where, expr, families)

    def test_counters_are_named_total_and_gauges_are_not(self):
        check_names(declared_families())

    def test_the_up_alert_names_a_configured_job(self):
        jobs = scrape_jobs()
        named = [
            job
            for _, expr in alert_expressions()
            for job in UP_JOB_RE.findall(expr)
        ]
        self.assertTrue(named, "no alert uses `up`; the job check would pass vacuously")
        for job in named:
            self.assertIn(job, jobs, f"alert watches job {job!r}, scrape config has {jobs}")

    def test_the_scrape_config_loads_its_rule_file(self):
        document = yaml.safe_load(SCRAPE_CONFIG.read_text())
        files = document.get("rule_files") or []
        self.assertTrue(files, "prometheus.yml does not load any rule file")
        for entry in files:
            resolved = (SCRAPE_CONFIG.parent / entry).resolve()
            self.assertTrue(resolved.is_file(), f"rule_files entry {entry!r} does not exist")

    # The guard's own inversion: a check that catches nothing must fail here.

    def test_an_unknown_family_is_rejected(self):
        families = declared_families()
        with self.assertRaisesRegex(AssertionError, "does not declare"):
            check_expr("test", "yadb_gc_no_such_family > 0", families)

    def test_rate_over_a_gauge_is_rejected(self):
        families = declared_families()
        with self.assertRaisesRegex(AssertionError, "applies a rate"):
            check_expr("test", "rate(yadb_gc_fragments_deleted[5m]) > 0", families)

    def test_rate_over_a_counter_is_accepted(self):
        families = declared_families()
        check_expr("test", "rate(yadb_pool_refused_puts_total[5m]) > 0", families)

    def test_a_gauge_named_total_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "ends in _total"):
            check_names({"yadb_wrong_total": "gauge"})

    def test_a_counter_without_total_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "does not end in _total"):
            check_names({"yadb_wrong": "counter"})


if __name__ == "__main__":
    unittest.main()
