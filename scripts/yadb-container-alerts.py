#!/usr/bin/env python3
"""Evaluate every shipped alert rule against a real three-node Multi-Raft scrape.

`scripts/test_metrics_alert_rules.py` is a static guard: it proves that every
expression names a family the exposition declares and that `rate()` is never
applied to a per-pass gauge. It cannot prove that a rule is *silent on a healthy
cluster*, because a rule whose expression is syntactically fine and refers to a
real family can still be true on a perfectly healthy deployment. That is what
this harness measures.

It reuses `scripts/yadb-container-multinode.py` unchanged to bring up three
host-built daemons in three rootless Podman containers on one bridge (distinct
IPs and network namespaces, one HTTP port each published on loopback), then:

1. scrapes `GET /metrics` from all three nodes and parses the Prometheus text
   exposition with a parser written here (no `prometheus_client` dependency);
2. evaluates every `expr` in `deploy/monitoring/yadb-alerts.yml` over the
   scraped samples, in PromQL's own vector semantics, and asserts that nothing
   is firing on the healthy cluster. A rule whose expression is true is reported
   with the series that made it true, so "no alert" cannot be confused with
   "the query matched nothing by accident";
3. drives a rule's expression true through the HTTP surface. The reachable
   mechanism is a garbage-collection refusal: a real SQL read publishes a
   committed reader lease, and `POST /v1/pools/gc` then refuses with HTTP 409
   and `yadb_gc_leases_live 1`, which is exactly what `YadbGcRefused` watches.
   The lease is allowed to expire and a second pass is made, so the family is
   shown to track a live condition rather than a sticky flag;
4. stops one node's daemon and holds the cluster in that state for longer than
   `YadbNodeDown`'s `for:`, evaluating the rule at both ends of the window. That
   is the only rule evaluated over time, and it is the rule this harness drives
   into *firing* rather than merely true;
5. runs three inversions that feed perturbed input to the same assertions and
   print the exact failure text, so a check that had become vacuous would fail
   the run: a scrape of a port with no daemon, an empty exposition and a
   family-stripped one, and the firing assertion applied to the healthy scrape.

Two limits are stated rather than hidden. `for:` is honoured, so a rule whose
expression is true but whose `for:` has not elapsed is reported `pending`, not
`firing`. And three shipped rules are permanently true on some node of a healthy
cluster - a follower never runs a pass, and the leader's pass refuses the groups
it does not lead - so the healthy assertion prints them as findings with the
measured evidence instead of pretending they are silent.

Three containers on one kernel is not a multi-host deployment. One scrape is not
a time series. No Alertmanager routing, inhibition or silencing is exercised:
this harness evaluates expressions, it does not run Prometheus.
"""
import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import yaml

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    'yadb_multinode', REPO / 'scripts' / 'yadb-container-multinode.py')
MN = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(MN)
DEV = MN.DEV
NODES = MN.NODES

ALERTS = REPO / 'deploy' / 'monitoring' / 'yadb-alerts.yml'
SCRAPE_CONFIG = REPO / 'deploy' / 'monitoring' / 'prometheus.yml'

# Families the daemon emits only after the matching pass has run. Their gate
# (`*_last_pass_present`) is always present, so an absent family here is
# explainable only when the gate is 0. That is the check inversion (b) attacks.
GC_PASS_ONLY = frozenset({
    'yadb_gc_blobs_live', 'yadb_gc_fragments_deleted', 'yadb_gc_bytes_reclaimed',
    'yadb_gc_leases_live', 'yadb_gc_barriers_applied', 'yadb_gc_refused',
    'yadb_gc_groups_refused', 'yadb_gc_pools_refused', 'yadb_gc_errors',
})
SCRUB_PASS_ONLY = frozenset({
    'yadb_scrub_pools_checked', 'yadb_scrub_blobs_checked', 'yadb_scrub_blobs_intact',
    'yadb_scrub_blobs_repaired', 'yadb_scrub_fragments_repaired',
    'yadb_scrub_blobs_unrecoverable', 'yadb_scrub_refused_puts_total',
    'yadb_scrub_degraded_puts_total', 'yadb_scrub_orphan_blobs_collected',
    'yadb_scrub_repair_error', 'yadb_scrub_orphan_collection_error',
})
# Declared unconditionally but sampled only in a failure branch: the exposition
# emits one row per metadata group when enumeration succeeds, and this family
# only when it does not. Its gate is therefore the presence of `yadb_group_info`.
ERROR_ONLY = frozenset({'yadb_group_observation_error'})

# Every rule this harness knows how to explain. A rule added to the file without
# an entry here fails the run rather than being skipped silently.
EXPECTED_RULES = (
    'YadbNodeDown', 'YadbGcNoPass', 'YadbGcRefused', 'YadbGcErrors',
    'YadbPoolNoOnlineSlots', 'YadbPoolBelowWriteQuorum', 'YadbSlotOffline',
    'YadbScrubNoPass', 'YadbScrubUnrecoverable', 'YadbScrubFailed',
    'YadbGroupObservationFailed', 'YadbGroupError',
)

# A rule whose expression is true on a *healthy* cluster is either a genuine
# finding or a rule this harness is wrong about. Anything not listed here fails
# the healthy-cluster assertion, so a new false positive cannot be absorbed
# silently. The entry records the mechanism, and the run fails if the mechanism
# stops reproducing, so the list cannot outlive the defect it documents.
KNOWN_TRUE_ON_HEALTHY = {
    'YadbGcNoPass': 'the gate is 0 on all three nodes at this point. The control-group leader '
                    'runs a collection pass every BACKGROUND_SWEEP_INTERVAL (300 s), so its gate '
                    'becomes 1 within five minutes; the two followers never run a pass, so their '
                    'gate stays 0 and this rule fires on them one hour after startup on an '
                    'otherwise healthy cluster',
    'YadbScrubNoPass': 'the repair scan runs only while the node believes it leads the control '
                       'group, so the two followers never take a pass and their gate is '
                       'permanently 0; this rule fires on them 24 h after startup on a healthy '
                       'cluster',
    'YadbScrubFailed': 'the control-group leader takes a pass every few seconds, and that pass '
                       'refuses every metadata group it does not lead '
                       '(gc.rs repair_group: "node N is not the leader of metadata group G ..."), '
                       'which main.rs records as yadb_scrub_repair_error 1 and '
                       'yadb_scrub_orphan_collection_error 1. This rule therefore fires on the '
                       'leader 10 minutes after startup on a healthy cluster',
}


# --------------------------------------------------------------------------- #
# Prometheus text exposition parser
# --------------------------------------------------------------------------- #

LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
SAMPLE_RE = re.compile(
    r'^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?P<labels>\{[^}]*\})?\s+(?P<value>\S+)$')
UNESCAPE = {'\\': '\\', '"': '"', 'n': '\n'}


def unescape(value):
    return re.sub(r'\\([\\"n])', lambda match: UNESCAPE[match.group(1)], value)


class Sample:
    """One exposition sample, plus the target it was scraped from."""

    __slots__ = ('name', 'labels', 'value')

    def __init__(self, name, labels, value):
        self.name, self.labels, self.value = name, labels, value

    @property
    def key(self):
        return tuple(sorted(self.labels.items()))

    def __repr__(self):
        inner = ','.join(f'{k}="{v}"' for k, v in sorted(self.labels.items()))
        return f'{self.name}{{{inner}}} {self.value:g}'


def parse_value(text):
    lowered = text.lower()
    if lowered == 'nan':
        return math.nan
    if lowered in ('+inf', 'inf'):
        return math.inf
    if lowered == '-inf':
        return -math.inf
    return float(text)


def parse_exposition(text, instance):
    """Parse a Prometheus text exposition into samples labelled with `instance`.

    The format is deliberately small: comments, `# TYPE` lines, and
    `name{labels} value` samples. Anything else is a parse failure, because a
    silently skipped line would look like an absent family.
    """
    samples, types = [], {}
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        if line.startswith('#'):
            parts = line.split(None, 3)
            if len(parts) >= 3 and parts[1] == 'TYPE':
                types[parts[2]] = parts[3] if len(parts) > 3 else ''
            continue
        match = SAMPLE_RE.match(line)
        if match is None:
            raise ValueError(f'{instance}: unparseable exposition line {number}: {line!r}')
        labels = {'instance': instance}
        for key, value in LABEL_RE.findall(match.group('labels') or ''):
            labels[key] = unescape(value)
        samples.append(Sample(match.group('name'), labels, parse_value(match.group('value'))))
    return samples, types


class Scrape:
    """The result of scraping every configured target once.

    `up` is not served by the daemon: Prometheus synthesises it per target. It is
    synthesised here the same way (1 for a scrape that produced samples, 0 for a
    failed one), and nothing else is invented for a failed target.
    """

    def __init__(self, job):
        self.job = job
        self.targets = {}

    def add(self, instance, samples, types):
        self.targets[instance] = {'samples': samples, 'types': types, 'error': None}

    def fail(self, instance, error):
        self.targets[instance] = {'samples': [], 'types': {}, 'error': error}

    def samples(self):
        merged = []
        for instance, entry in self.targets.items():
            merged.extend(entry['samples'])
            merged.append(Sample('up', {'job': self.job, 'instance': instance},
                                 0.0 if entry['error'] else 1.0))
        return merged

    def types(self):
        merged = {}
        for entry in self.targets.values():
            merged.update(entry['types'])
        return merged

    def failed(self):
        return {instance: entry['error']
                for instance, entry in self.targets.items() if entry['error']}

    def of(self, name):
        return [sample for sample in self.samples() if sample.name == name]

    def value(self, name, instance, **labels):
        for sample in self.samples():
            if (sample.name == name and sample.labels.get('instance') == instance
                    and all(sample.labels.get(key) == value for key, value in labels.items())):
                return sample.value
        return None


def instance_of(base, node):
    """The scrape target of one node, as Prometheus would label it."""
    return f'127.0.0.1:{DEV.http_port(base, node)}'


def scrape(cluster, base, job, nodes=NODES, timeout=30):
    result = Scrape(job)
    for node in nodes:
        instance = instance_of(base, node)
        try:
            request = urllib.request.Request(f'http://{instance}/metrics')
            with urllib.request.urlopen(request, timeout=timeout) as response:
                if response.status != 200:
                    raise RuntimeError(f'HTTP {response.status}')
                body = response.read().decode('utf-8')
            samples, types = parse_exposition(body, instance)
        except (OSError, ValueError, RuntimeError, urllib.error.URLError) as error:
            result.fail(instance, f'{type(error).__name__}: {error}')
            continue
        if not samples:
            result.fail(instance, 'the scrape returned no samples')
            continue
        result.add(instance, samples, types)
    return result


# --------------------------------------------------------------------------- #
# A small PromQL subset: selectors, comparisons, and/or, scalars
# --------------------------------------------------------------------------- #

TOKEN_RE = re.compile(r'''
    (?P<space>\s+)
  | (?P<duration>\d+(?:ms|s|m|h|d|w|y))
  | (?P<number>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
  | (?P<ident>[a-zA-Z_:][a-zA-Z0-9_:]*)
  | (?P<op>==|!=|>=|<=|=~|!~|>|<)
  | (?P<string>"(?:[^"\\]|\\.)*")
  | (?P<punct>[{}\[\](),=])
''', re.X)

DURATIONS = {'ms': .001, 's': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800, 'y': 31536000}


def tokenize(expr):
    tokens, position = [], 0
    while position < len(expr):
        match = TOKEN_RE.match(expr, position)
        if match is None:
            raise ValueError(f'cannot tokenize {expr!r} at offset {position}: {expr[position:]!r}')
        position = match.end()
        if match.lastgroup == 'space':
            continue
        tokens.append((match.lastgroup, match.group()))
    return tokens


class Parser:
    """Recursive descent over the subset the shipped rules use."""

    def __init__(self, expr):
        self.expr = expr
        self.tokens = tokenize(expr)
        self.position = 0

    def peek(self):
        return self.tokens[self.position] if self.position < len(self.tokens) else (None, None)

    def take(self, expected=None):
        token = self.peek()
        if token[0] is None:
            raise ValueError(f'{self.expr!r}: unexpected end of expression')
        if expected is not None and token[1] != expected:
            raise ValueError(f'{self.expr!r}: expected {expected!r}, found {token[1]!r}')
        self.position += 1
        return token

    def parse(self):
        node = self.parse_or()
        if self.peek()[0] is not None:
            raise ValueError(f'{self.expr!r}: trailing token {self.peek()[1]!r}')
        return node

    def parse_or(self):
        node = self.parse_and()
        while self.peek() == ('ident', 'or'):
            self.take()
            node = ('or', node, self.parse_and())
        return node

    def parse_and(self):
        node = self.parse_comparison()
        while self.peek() == ('ident', 'and'):
            self.take()
            node = ('and', node, self.parse_comparison())
        return node

    def parse_comparison(self):
        node = self.parse_unary()
        if self.peek()[0] == 'op':
            operator = self.take()[1]
            node = ('cmp', operator, node, self.parse_unary())
        return node

    def parse_unary(self):
        kind, text = self.peek()
        if kind == 'number':
            self.take()
            return ('num', float(text))
        if kind == 'ident' and text in ('rate', 'irate', 'increase', 'delta'):
            raise ValueError(
                f'{self.expr!r}: {text}() needs two samples in time; this harness has one scrape, '
                'and a rate computed from a single scrape would be a fabricated series')
        if kind != 'ident':
            raise ValueError(f'{self.expr!r}: expected a selector or number, found {text!r}')
        name = self.take()[1]
        matchers = []
        if self.peek()[1] == '{':
            self.take('{')
            while self.peek()[1] != '}':
                key = self.take()[1]
                operator = self.take()[1]
                if operator not in ('=', '!=', '=~', '!~'):
                    raise ValueError(f'{self.expr!r}: unsupported matcher {key}{operator}')
                value_kind, value_text = self.take()
                if value_kind == 'string':
                    value_text = unescape(value_text[1:-1])
                matchers.append((key, operator, value_text))
                if self.peek()[1] == ',':
                    self.take()
            self.take('}')
        return ('sel', name, tuple(matchers))


def parse_expression(expr):
    return Parser(expr).parse()


def select(samples, name, matchers):
    out = []
    for sample in samples:
        if sample.name != name:
            continue
        for key, operator, value in matchers:
            actual = sample.labels.get(key)
            if operator == '=' and actual != value:
                break
            if operator == '!=' and actual == value:
                break
            if operator in ('=~', '!~'):
                raise ValueError('regex matchers are not used by the shipped rules; refusing')
        else:
            out.append(sample)
    return out


def compare(operator, left, right):
    if operator == '==':
        return left == right
    if operator == '!=':
        return left != right
    if operator == '>':
        return left > right
    if operator == '<':
        return left < right
    if operator == '>=':
        return left >= right
    if operator == '<=':
        return left <= right
    raise ValueError(f'unsupported operator {operator}')


def evaluate(node, samples):
    """Evaluate one AST node; returns a list of Samples (a vector) or a float."""
    kind = node[0]
    if kind == 'num':
        return node[1]
    if kind == 'sel':
        return select(samples, node[1], node[2])
    if kind in ('and', 'or'):
        left, right = evaluate(node[1], samples), evaluate(node[2], samples)
        if isinstance(left, float) or isinstance(right, float):
            raise ValueError(f'{kind} needs two vectors, got a scalar')
        right_keys = {sample.key for sample in right}
        if kind == 'and':
            return [sample for sample in left if sample.key in right_keys]
        seen, merged = set(), []
        for sample in left + right:
            if sample.key not in seen:
                seen.add(sample.key)
                merged.append(sample)
        return merged
    if kind == 'cmp':
        _, operator, left_node, right_node = node
        left, right = evaluate(left_node, samples), evaluate(right_node, samples)
        if isinstance(left, float) and isinstance(right, float):
            return 1.0 if compare(operator, left, right) else 0.0
        if isinstance(right, float):
            return [sample for sample in left if compare(operator, sample.value, right)]
        if isinstance(left, float):
            return [sample for sample in right if compare(operator, left, sample.value)]
        right_by_key = {sample.key: sample.value for sample in right}
        out = []
        for sample in left:
            other = right_by_key.get(sample.key)
            if other is not None and compare(operator, sample.value, other):
                out.append(sample)
        return out
    raise ValueError(f'unsupported expression node {kind!r}')


# --------------------------------------------------------------------------- #
# Rules
# --------------------------------------------------------------------------- #

def load_rules():
    document = yaml.safe_load(ALERTS.read_text())
    rules = []
    for group in document['groups']:
        for rule in group['rules']:
            rules.append({
                'group': group['name'],
                'name': rule['alert'],
                'expr': rule['expr'],
                'for': rule.get('for', '0s'),
                'severity': rule.get('labels', {}).get('severity'),
            })
    return rules


def for_seconds(text):
    match = re.fullmatch(r'(\d+)(ms|s|m|h|d|w|y)', str(text).strip())
    if match is None:
        raise ValueError(f'cannot read a `for:` duration from {text!r}')
    return int(match.group(1)) * DURATIONS[match.group(2)]


def scrape_job():
    document = yaml.safe_load(SCRAPE_CONFIG.read_text())
    jobs = [entry['job_name'] for entry in document['scrape_configs']]
    if len(jobs) != 1:
        raise RuntimeError(f'expected one scrape job in {SCRAPE_CONFIG}, found {jobs}')
    return jobs[0]


class RuleResult:
    def __init__(self, rule, matched, observed_for):
        self.rule = rule
        self.matched = matched
        self.true = bool(matched)
        self.for_seconds = for_seconds(rule['for'])
        # `observed_for` is how long this run has *proved* the expression true:
        # 0 for a single scrape, the bracketing window for the held node-down
        # phase. A rule can only be called firing when that window covers `for`.
        self.observed_for = observed_for
        if not self.true:
            self.state = 'inactive'
        elif self.for_seconds == 0 or self.for_seconds <= observed_for:
            self.state = 'firing'
        else:
            self.state = 'pending'

    def summary(self):
        instances = sorted({sample.labels.get('instance', '?') for sample in self.matched})
        return ', '.join(instances) if instances else '-'


def evaluate_rules(rules, samples, observed_for=None):
    results = []
    for rule in rules:
        matched = evaluate(parse_expression(rule['expr']), samples)
        if isinstance(matched, float):
            matched = [Sample('scalar', {}, matched)] if matched else []
        observed = (observed_for or {}).get(rule['name'], 0)
        results.append(RuleResult(rule, matched, observed))
    return results


def report_table(results, title):
    print(f'\n{title}')
    print(f'  {"rule":32} {"state":9} {"for":5} instances')
    for result in results:
        print(f'  {result.rule["group"] + "/" + result.rule["name"]:32} '
              f'{result.state:9} {result.rule["for"]:5} {result.summary()}')


# --------------------------------------------------------------------------- #
# Per-rule evidence: why the expression is silent, measured
# --------------------------------------------------------------------------- #

def _num(value):
    return 'absent' if value is None else f'{value:g}'


def per_instance(scrape_set, family, base):
    return ', '.join(
        f'node {node}={_num(scrape_set.value(family, instance_of(base, node)))}'
        for node in NODES)


def tally(scrape_set, family, want):
    """'N of M <family> samples are <want>', measured, so a note cannot lie."""
    samples = scrape_set.of(family)
    matching = sum(1 for sample in samples if sample.value == want)
    return f'{matching} of {len(samples)} {family} samples are {want:g}'


def gate_state(scrape_set, family, base):
    """Which nodes report a non-zero gate, so an absence claim is falsifiable."""
    live = [node for node in NODES
            if scrape_set.value(family, instance_of(base, node)) not in (None, 0)]
    return f'gate set on nodes {live} of {list(NODES)}' if live else \
        f'gate 0 on all {len(NODES)} nodes'


def evidence(scrape_set, base):
    """Measured reason each rule is silent or true, keyed by rule name."""
    def pools(family):
        seen = {}
        for sample in scrape_set.of(family):
            seen.setdefault(sample.labels.get('pool'), set()).add(sample.value)
        return seen

    online, minimum = pools('yadb_pool_online_slots'), pools('yadb_pool_min_write_slots')
    return {
        'YadbNodeDown': 'every configured target answered its scrape: '
                        + ', '.join(f'{sample.labels["instance"]}={_num(sample.value)}'
                                    for sample in scrape_set.of('up')),
        'YadbGcNoPass': 'yadb_gc_last_pass_present '
                        + per_instance(scrape_set, 'yadb_gc_last_pass_present', base)
                        + ' (' + gate_state(scrape_set, 'yadb_gc_last_pass_present', base) + '); '
                        'a node that has taken no pass has an empty yadb_gc_* sample set',
        'YadbGcRefused': 'yadb_gc_refused '
                         + per_instance(scrape_set, 'yadb_gc_refused', base)
                         + '; a node with yadb_gc_last_pass_present 0 has no pass to report, so '
                         'the comparison sees an empty vector rather than a zero',
        'YadbGcErrors': 'yadb_gc_errors '
                        + per_instance(scrape_set, 'yadb_gc_errors', base)
                        + '; absent for the same reason: no collection pass has run',
        'YadbPoolNoOnlineSlots': 'every pool reports reachable slots: ' + ', '.join(
            f'{pool}={sorted(values)}' for pool, values in sorted(online.items())),
        'YadbPoolBelowWriteQuorum': 'online equals the write quorum on every pool: ' + ', '.join(
            f'{pool}: online={sorted(online.get(pool, []))} '
            f'min_write={sorted(minimum.get(pool, []))}' for pool in sorted(online)),
        'YadbSlotOffline': tally(scrape_set, 'yadb_slot_online', 1.0),
        'YadbScrubNoPass': 'yadb_scrub_last_pass_present '
                           + per_instance(scrape_set, 'yadb_scrub_last_pass_present', base)
                           + ' (' + gate_state(scrape_set, 'yadb_scrub_last_pass_present', base)
                           + '); the periodic scan runs only while a node believes it leads the '
                           'control group, so a follower has no pass to report',
        'YadbScrubUnrecoverable': 'yadb_scrub_blobs_unrecoverable is 0 wherever a scan has run ('
                                  + per_instance(scrape_set, 'yadb_scrub_blobs_unrecoverable', base)
                                  + ')',
        'YadbScrubFailed': 'yadb_scrub_repair_error '
                           + per_instance(scrape_set, 'yadb_scrub_repair_error', base)
                           + '; yadb_scrub_orphan_collection_error '
                           + per_instance(scrape_set, 'yadb_scrub_orphan_collection_error', base),
        'YadbGroupObservationFailed': 'yadb_group_observation_error is absent on every node ('
                                      + per_instance(scrape_set, 'yadb_group_observation_error',
                                                     base)
                                      + f') while {len(scrape_set.of("yadb_group_info"))} group '
                                      'rows exist, so enumeration succeeded and the error row was '
                                      'never emitted',
        'YadbGroupError': tally(scrape_set, 'yadb_group_error', 0.0),
    }


# --------------------------------------------------------------------------- #
# Assertions
# --------------------------------------------------------------------------- #

class CheckFailure(AssertionError):
    pass


def check(condition, message):
    if not condition:
        raise CheckFailure(message)


def check_scrape_complete(scrape_set, expected_targets):
    """A target with no samples is a failed run, never a quiet empty result."""
    check(not scrape_set.failed(),
          'scrape failed for target(s) '
          + ', '.join(f'{instance} ({error})'
                      for instance, error in scrape_set.failed().items())
          + '; a failed scrape is not an empty exposition')
    for instance, entry in scrape_set.targets.items():
        check(entry['samples'], f'{instance} returned no samples')
    check(len(scrape_set.targets) == expected_targets,
          f'expected {expected_targets} targets, scraped {len(scrape_set.targets)}')
    check(scrape_set.types(), 'the exposition declared no families at all')


def check_family_coverage(scrape_set):
    """Every declared family is present, or absent for a stated gate reason.

    This is the inversion-(b) target: on an empty scrape the expected set is
    every declared family, so an empty result fails instead of passing because
    "there was nothing to complain about".
    """
    declared = set(scrape_set.types())
    check(declared, 'no family is declared in the exposition')
    present = {sample.name for sample in scrape_set.samples()} - {'up'}
    unexplained = []
    for family in sorted(declared - present):
        if family in GC_PASS_ONLY:
            gates = scrape_set.of('yadb_gc_last_pass_present')
            if gates and all(sample.value == 0 for sample in gates):
                continue
        if family in SCRUB_PASS_ONLY:
            gates = scrape_set.of('yadb_scrub_last_pass_present')
            if gates and all(sample.value == 0 for sample in gates):
                continue
        if family in ERROR_ONLY:
            # The error row exists only when group enumeration failed; rows for
            # the groups themselves prove it did not.
            if scrape_set.of('yadb_group_info'):
                continue
        unexplained.append(family)
    check(not unexplained,
          'declared families absent with no gate explaining them: ' + ', '.join(unexplained)
          + ' (the gate is 1 or itself missing, so the family is silently gone)')
    unknown = present - declared
    check(not unknown, 'samples of undeclared families: ' + ', '.join(sorted(unknown)))
    return sorted(declared)


def check_rules_known(rules):
    names = [rule['name'] for rule in rules]
    check(len(names) == len(set(names)), f'duplicate rule names: {names}')
    check(set(names) == set(EXPECTED_RULES),
          'the shipped rule set changed: missing '
          f'{sorted(set(EXPECTED_RULES) - set(names))}, new '
          f'{sorted(set(names) - set(EXPECTED_RULES))}. Add an evidence note for each new rule '
          'instead of letting it go unevaluated')
    job = scrape_job()
    for rule in rules:
        for named in re.findall(r'\bup\s*\{[^}]*\bjob\s*=\s*"([^"]+)"', rule['expr']):
            check(named == job,
                  f'{rule["name"]} watches job {named!r}, the scrape config says {job!r}')


def check_healthy(results, scrape_set):
    """Nothing is firing, and every true expression is a documented exception.

    The non-vacuity guard comes first. Every rule is silent when the sample set
    is empty, so "no alert" would be true for the worst possible reason. These
    families are emitted on every scrape regardless of any pass, so their
    absence is a broken scrape, not a quiet cluster.
    """
    for family in ('yadb_node_info', 'yadb_pool_online_slots', 'yadb_pool_min_write_slots',
                   'yadb_slot_online', 'yadb_group_info', 'yadb_gc_last_pass_present',
                   'yadb_scrub_last_pass_present', 'up'):
        check(scrape_set.of(family),
              f'{family} has no samples: the healthy-cluster assertion would be vacuous, and an '
              'absent family is not a silent cluster')
    reporting = {sample.labels.get('instance') for sample in scrape_set.of('yadb_node_info')}
    scraped = {sample.labels.get('instance') for sample in scrape_set.of('up')}
    check(reporting == scraped,
          f'targets that answered a scrape without yadb_node_info: {sorted(scraped - reporting)}')

    firing = [result for result in results if result.state == 'firing']
    check(not firing, 'rules firing on a healthy cluster: '
                      + ', '.join(f'{r.rule["name"]} ({r.matched})' for r in firing))
    true_names = {result.rule['name'] for result in results if result.true}
    unexpected = true_names - set(KNOWN_TRUE_ON_HEALTHY)
    check(not unexpected,
          'expression(s) true on a healthy cluster with no documented mechanism: '
          + ', '.join(sorted(unexpected))
          + '. Either the cluster is unhealthy or the rule is a false positive; do not add it '
            'to KNOWN_TRUE_ON_HEALTHY without a measured mechanism')
    stale = set(KNOWN_TRUE_ON_HEALTHY) - true_names
    check(not stale,
          'documented healthy-cluster exception(s) no longer true: ' + ', '.join(sorted(stale))
          + '; remove them from KNOWN_TRUE_ON_HEALTHY so the harness keeps catching them')
    print('\nhealthy-cluster assertion: no rule is firing at the moment of measurement')
    for name in sorted(true_names):
        print(f'  FINDING {name} is true but still pending its for:')
        print(f'    {KNOWN_TRUE_ON_HEALTHY[name]}')


# --------------------------------------------------------------------------- #
# Driving a real rule
# --------------------------------------------------------------------------- #

def container_pids(cluster, node, process='yadbd'):
    script = ('for p in /proc/[0-9]*; do '
              f'[ "$(cat "$p/comm" 2>/dev/null)" = {process} ] && echo "${{p#/proc/}}"; done')
    result = cluster.exec(node, 'bash', '-c', script, check=False)
    return [int(line) for line in result.stdout.split() if line.isdigit()]


def kill_daemon(cluster, node):
    pids = container_pids(cluster, node)
    check(len(pids) == 1, f'expected exactly one yadbd in container {node}, found {pids}')
    cluster.exec(node, 'bash', '-c', f'kill -TERM {pids[0]}', check=False)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and container_pids(cluster, node):
        time.sleep(.2)
    check(not container_pids(cluster, node),
          f'node {node} daemon {pids[0]} did not exit on SIGTERM')
    print(f'node {node}: daemon PID {pids[0]} stopped; its container and netns remain')
    return pids[0]


def signed_post(port, path, body, secret, subject, timeout=60):
    headers = dict(DEV.signed_headers(secret, subject))
    headers['Content-Type'] = 'application/json'
    request = urllib.request.Request(
        f'http://127.0.0.1:{port}{path}', data=json.dumps(body).encode(),
        headers=headers, method='POST')
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode(errors='replace')
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode(errors='replace')


def wait_for(description, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, ValueError, KeyError, RuntimeError, urllib.error.URLError) as error:
            last = f'{type(error).__name__}: {error}'
        time.sleep(.5)
    raise CheckFailure(f'{description}: timed out; last={last!r}')


def rule_named(results, name):
    for result in results:
        if result.rule['name'] == name:
            return result
    raise CheckFailure(f'no rule named {name}')


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port-base', type=int, default=24800,
                        help='base of the derived listener block (default 24800)')
    parser.add_argument('--image', default='ubuntu:26.04')
    parser.add_argument('--hold-secs', type=int, default=130,
                        help='how long the down node is held, so YadbNodeDown outlives its for: '
                             '(default 130)')
    parser.add_argument('--lease-wait-secs', type=int, default=45,
                        help='how long to wait for the reader lease to expire (default 45)')
    args = parser.parse_args()
    DEV.validate_base(args.port_base)
    if not DEV.DAEMON.is_file():
        raise RuntimeError(f'build the host daemon first: {DEV.DAEMON}')
    job = scrape_job()
    rules = load_rules()
    check_rules_known(rules)
    base = args.port_base

    started = time.monotonic()
    with tempfile.TemporaryDirectory(
            prefix='yadb-alerts-',
            dir=os.environ.get('YADB_TEST_TMPDIR', '/tmp/opencode')) as directory:
        cluster = MN.Cluster(Path(directory), base, args.image)
        try:
            print(f'scrape job {job!r}; {len(rules)} rules from '
                  f'{ALERTS.relative_to(REPO)}', flush=True)
            cluster.setup()
            cluster.apply(1, DEV.PLATFORM.read_text())
            print('waiting for both pools to report their full slot set on every node', flush=True)

            def pools_ready():
                scrape_set = scrape(cluster, base, job)
                if scrape_set.failed():
                    return False
                for node in NODES:
                    instance = instance_of(base, node)
                    for pool in ('wal-fast', 'data-ec'):
                        online = scrape_set.value('yadb_pool_online_slots', instance, pool=pool)
                        total = scrape_set.value('yadb_pool_slots', instance, pool=pool)
                        if online is None or total is None or online != total or online == 0:
                            return False
                return scrape_set

            ready = wait_for('every node reports fully online pools', pools_ready, 90)
            # The healthy snapshot must be the pre-collection state: a background
            # sweep runs 300 s after start and would make YadbGcRefused true for a
            # reason this phase is not testing.
            wait_for('the periodic repair scan to run on the control-group leader',
                     lambda: any(sample.value == 1 for sample in
                                 scrape(cluster, base, job).of('yadb_scrub_last_pass_present')), 30)
            ready = scrape(cluster, base, job)
            gates = ready.of('yadb_gc_last_pass_present')
            check(gates and all(sample.value == 0 for sample in gates),
                  'a collection pass already ran before the healthy snapshot '
                  f'({[str(s) for s in gates]}); this phase needs the pre-pass state')
            print(f'healthy scrape: {sum(len(e["samples"]) for e in ready.targets.values())} '
                  f'samples from {len(ready.targets)} nodes', flush=True)

            # ---- phase 1: the healthy cluster ------------------------------- #
            check_scrape_complete(ready, len(NODES))
            declared = check_family_coverage(ready)
            print(f'{len(declared)} families declared and accounted for')
            results = evaluate_rules(rules, ready.samples())
            notes = evidence(ready, base)
            check_healthy(results, ready)
            report_table(results, 'every shipped rule, healthy three-node cluster')
            for result in results:
                print(f'    {result.rule["name"]}: {result.rule["expr"]}')
                print(f'      measured: {notes[result.rule["name"]]}')

            # ---- phase 2: drive a real GC refusal --------------------------- #
            # A read as the platform's readWrite subject publishes a committed
            # reader lease in the metadata group that owns project `billing`
            # (group 2). A collection pass refuses while that lease is live. The
            # read and the pass both go to the group's leader, because the lease
            # fence is evaluated against that leader's applied state.
            def group_leader(group):
                for node in NODES:
                    for observed in cluster.status(node):
                        if observed['group'] == group and observed['leader_id'] == node:
                            return node
                return None

            gc_node = wait_for('some node leads metadata group 2', lambda: group_leader(2))
            gc_port = DEV.http_port(base, gc_node)
            gc_instance = instance_of(base, gc_node)
            print(f'\nmetadata group 2 is led by node {gc_node}', flush=True)

            status, body = signed_post(gc_port, '/db/billing/ledger/query',
                                       {'sql': 'SELECT 1'}, cluster.secret, DEV.MEMBER_SUBJECT)
            check(status == 200, f'the lease-publishing read failed: HTTP {status}: {body}')
            print(f'read as {DEV.MEMBER_SUBJECT} on node {gc_node}: HTTP {status} {body}',
                  flush=True)

            status, body = signed_post(gc_port, '/v1/pools/gc', {}, cluster.secret, DEV.ADMIN)
            check(status == 409, 'POST /v1/pools/gc should refuse while a reader lease is live, '
                                 f'got HTTP {status}: {body}')
            refusal = json.loads(body)
            check(refusal.get('leases_live', 0) >= 1,
                  f'the refusal does not name a live lease: {body}')
            print(f'POST /v1/pools/gc on node {gc_node}: HTTP 409, '
                  f'leases_live={refusal["leases_live"]}, '
                  f'groups_refused={len(refusal.get("gc_groups_refused") or [])}')
            print(f'  gc_refusal: {refusal.get("gc_refusal")}')

            driven = scrape(cluster, base, job)
            check_scrape_complete(driven, len(NODES))
            driven_results = evaluate_rules(rules, driven.samples())
            refused = rule_named(driven_results, 'YadbGcRefused')
            check(refused.true,
                  'YadbGcRefused did not become true after a refused collection pass: '
                  f'{refused.rule["expr"]!r} matched nothing; yadb_gc_refused on node '
                  f'{gc_node} is {_num(driven.value("yadb_gc_refused", gc_instance))}')
            check(refused.summary() == gc_instance,
                  f'YadbGcRefused is true on {refused.summary()}, expected only {gc_instance}')
            check(_num(driven.value('yadb_gc_leases_live', gc_instance)) == '1',
                  f'yadb_gc_leases_live on node {gc_node} is '
                  f'{_num(driven.value("yadb_gc_leases_live", gc_instance))}, expected 1')
            check(_num(driven.value('yadb_gc_last_pass_present', gc_instance)) == '1',
                  f'yadb_gc_last_pass_present on node {gc_node} did not become 1 after the pass')
            check(not rule_named(driven_results, 'YadbGcErrors').true,
                  'YadbGcErrors became true from a refusal; a refusal is not an error')
            print(f'YadbGcRefused: TRUE on {refused.summary()} (yadb_gc_refused=1, '
                  f'yadb_gc_leases_live=1), state {refused.state} until its 10m for: elapses; '
                  'YadbGcErrors stayed false')
            report_table(driven_results, 'rules after the refused collection pass')

            # The lease is transient: it expires one TTL (30 s) after the read, so
            # the *live* group gauge goes back to 0 and a later pass is refused
            # for leadership alone with leases_live 0. `yadb_gc_leases_live` is a
            # snapshot taken during a pass, so it only changes when a pass runs;
            # `yadb_group_reader_leases_live` is the live one.
            live_leases = driven.value('yadb_group_reader_leases_live', gc_instance, group='2')
            check(_num(live_leases) == '1',
                  f'yadb_group_reader_leases_live{{group="2"}} on node {gc_node} is '
                  f'{_num(live_leases)}, expected 1 while the read lease is live')
            wait_for('the reader lease to expire (yadb_group_reader_leases_live back to 0)',
                     lambda: scrape(cluster, base, job).value(
                         'yadb_group_reader_leases_live', gc_instance, group='2') == 0,
                     args.lease_wait_secs)
            status, body = signed_post(gc_port, '/v1/pools/gc', {}, cluster.secret, DEV.ADMIN)
            second = json.loads(body) if body.startswith('{') else {}
            check(status in (200, 409), f'the second pass returned HTTP {status}: {body}')
            check(second.get('leases_live', 0) == 0,
                  f'the lease did not clear: leases_live={second.get("leases_live")}')
            print(f'after one TTL: POST /v1/pools/gc HTTP {status}, leases_live=0, '
                  'so the earlier refusal was the lease and not a sticky flag')

            # ---- phase 3: one node down, held past YadbNodeDown's for: ------ #
            victim = 3
            instance3 = instance_of(base, victim)
            survivors = [node for node in NODES if node != victim]
            kill_daemon(cluster, victim)
            wait_for('the two survivors report a leader for all three groups',
                     lambda: all(all(g['leader_id'] is not None for g in cluster.status(node))
                                 for node in survivors), 60)
            print('survivors elected leaders for every group with one node down')

            def degraded_scrape():
                scrape_set = scrape(cluster, base, job, survivors)
                if scrape_set.failed():
                    return False
                for node in survivors:
                    instance = instance_of(base, node)
                    online = scrape_set.value('yadb_pool_online_slots', instance, pool='wal-fast')
                    if online is None or online >= 3:
                        return False
                return scrape_set

            degraded = wait_for('both survivors report the pool below its write quorum',
                                degraded_scrape, 60)
            # Prometheus records up=0 for a target whose scrape fails; that single
            # synthetic sample is the only thing added for the stopped node.
            degraded.fail(instance3, 'daemon stopped by this harness; no scrape was attempted')
            first_true = time.monotonic()
            down_results = evaluate_rules(rules, degraded.samples())
            node_down = rule_named(down_results, 'YadbNodeDown')
            check(node_down.true, 'YadbNodeDown did not become true with one node down')
            check(node_down.summary() == instance3,
                  f'YadbNodeDown is true on {node_down.summary()}, expected only {instance3}')
            quorum = rule_named(down_results, 'YadbPoolBelowWriteQuorum')
            check(quorum.true, 'YadbPoolBelowWriteQuorum did not become true with one node down: '
                               'online slots did not fall below the write quorum')
            print(f'\nnode {victim} down: YadbNodeDown TRUE on {node_down.summary()}; '
                  f'YadbPoolBelowWriteQuorum TRUE on {quorum.summary()}')

            # Hold the condition past `for: 2m` and evaluate again, so "firing"
            # means "true at both ends of a window longer than the for:".
            remaining = max(0, args.hold_secs - (time.monotonic() - first_true))
            print(f'holding the down node for {remaining:.0f}s more '
                  f'(YadbNodeDown for: {node_down.rule["for"]})', flush=True)
            time.sleep(remaining)
            held = scrape(cluster, base, job, survivors)
            check(not held.failed(),
                  f'a survivor scrape failed while holding the node down: {held.failed()}')
            held.fail(instance3, 'daemon stopped by this harness; no scrape was attempted')
            window = time.monotonic() - first_true
            held_results = evaluate_rules(rules, held.samples(), {'YadbNodeDown': window})
            held_down = rule_named(held_results, 'YadbNodeDown')
            check(held_down.true and held_down.summary() == instance3,
                  f'YadbNodeDown was not still true after {window:.0f}s on {instance3}')
            check(window >= held_down.for_seconds,
                  f'the observed window {window:.0f}s is shorter than the '
                  f'{held_down.rule["for"]} for:')
            print(f'YadbNodeDown: TRUE at both ends of a {window:.0f}s window on {instance3}, '
                  f'exceeding its {held_down.rule["for"]} for: -> {held_down.state}')

            firing = [result for result in held_results if result.state == 'firing']
            check([result.rule['name'] for result in firing] == ['YadbNodeDown'],
                  'expected exactly YadbNodeDown firing, got '
                  + ', '.join(f'{r.rule["name"]}({r.state})'
                              for r in held_results if r.true))
            report_table(held_results, f'rules with node {victim} down for {window:.0f}s')

            # ---- phase 4: prove the checks are load-bearing ------------------ #
            # Each inversion feeds a real perturbed input to the same function the
            # run above depends on, and records the exact failure text. A check
            # that stopped failing would fail the run here.
            def expect_failure(label, action, fragment):
                try:
                    action()
                except CheckFailure as error:
                    failure = str(error)
                    check(fragment in failure,
                          f'inversion {label} failed for an unexpected reason: {failure}')
                    print(f'  inversion {label}: {failure}')
                    return failure
                raise CheckFailure(
                    f'inversion {label} did not fail; the check it attacks is vacuous')

            print('\ninversions: each assertion above must fail on perturbed input')
            # (a) A port with no daemon must be a failed run, not a silent zero.
            dead = scrape(cluster, base, job, [victim])
            check(dead.failed(), f'node {victim} has no daemon but its scrape succeeded')
            expect_failure('(a) dead scrape target',
                           lambda: check_scrape_complete(dead, 1),
                           'scrape failed for target(s)')
            # (b) An empty or family-stripped sample set must fail the healthy
            # assertion instead of passing because nothing complained.
            expect_failure('(b) empty exposition',
                           lambda: check_family_coverage(Scrape(job)),
                           'no family is declared in the exposition')
            expect_failure('(b) empty sample set',
                           lambda: check_healthy([], Scrape(job)),
                           'has no samples: the healthy-cluster assertion would be vacuous')
            stripped = Scrape(job)
            for instance, entry in ready.targets.items():
                stripped.add(instance,
                             [sample for sample in entry['samples']
                              if sample.name != 'yadb_pool_online_slots'],
                             entry['types'])
            expect_failure('(b) dropped family',
                           lambda: check_family_coverage(stripped),
                           'declared families absent with no gate explaining them: '
                           'yadb_pool_online_slots')
            # (c) Without the down node the firing assertion must fail.
            healthy_down = rule_named(evaluate_rules(rules, ready.samples()), 'YadbNodeDown')
            check(not healthy_down.true, 'YadbNodeDown is true on the healthy scrape')
            expect_failure('(c) no firing scenario',
                           lambda: check(healthy_down.true,
                                         'YadbNodeDown did not become true with one node down'),
                           'YadbNodeDown did not become true with one node down')

            print('\nsummary')
            print('  healthy cluster: 0 rules firing at the measurement point; expressions true '
                  f'but pending: {sorted({r.rule["name"] for r in results if r.true})}')
            print(f'  refused pass:    YadbGcRefused true on node {gc_node} '
                  '(real HTTP 409, real reader lease)')
            print(f'  node down:       YadbNodeDown FIRING after {window:.0f}s '
                  f'> {held_down.rule["for"]}')
            print('  not proved:      multi-host placement, time-series behaviour beyond the two '
                  'bracketing samples above, Alertmanager routing/inhibition/silencing')
        finally:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            cluster.cleanup()
            remaining = cluster.podman(
                'ps', '-a', '--filter', f'label=yadb.test={cluster.label}',
                '--format', '{{.Names}}', check=False).stdout.strip()
            networks = cluster.podman(
                'network', 'ls', '--filter', f'label=yadb.test={cluster.label}',
                '--format', '{{.Name}}', check=False).stdout.strip()
            leftover = (f'containers remain after cleanup: {remaining}' if remaining
                        else f'networks remain after cleanup: {networks}' if networks else '')
            if leftover and sys.exc_info()[0] is not None:
                # Do not mask the failure that brought us here.
                print(f'CLEANUP FAILURE: {leftover}', file=sys.stderr)
            elif leftover:
                raise CheckFailure(leftover)
            else:
                print('cleanup verified: podman ps -a shows no test container, '
                      'no test network', flush=True)

    elapsed = time.monotonic() - started
    check(not Path(directory).exists(), f'the temporary directory {directory} was not removed')
    print(f'full run: {elapsed:.1f}s; temporary directory {directory} removed')


if __name__ == '__main__':
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except (RuntimeError, AssertionError, subprocess.SubprocessError, KeyboardInterrupt,
            urllib.error.URLError) as error:
        print(f'ERROR: {type(error).__name__}: {error}', file=sys.stderr)
        sys.exit(1)
