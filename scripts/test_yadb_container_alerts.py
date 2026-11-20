#!/usr/bin/env python3
"""Offline tests for `scripts/yadb-container-alerts.py`.

The live harness needs three containers and a built daemon, so its interesting
failure modes cannot be reached in a unit test. What can be pinned offline is
everything between the scrape and the verdict: the exposition parser, the
PromQL subset, `for:` arithmetic, and - most importantly - the three assertions
that would silently pass on degenerate input. A rule evaluated over an empty
sample set is false, so "no alert is firing" is trivially true when the scrape
is broken; each of those guards is inverted here and must fail.

Nothing here starts a container, reads `/metrics` or needs a daemon.
"""
import importlib.util
from pathlib import Path
import unittest

import yaml

spec = importlib.util.spec_from_file_location(
    'yadb_container_alerts', Path(__file__).with_name('yadb-container-alerts.py'))
alerts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(alerts)


def sample(name, value, **labels):
    return alerts.Sample(name, dict(labels), float(value))


EXPOSITION = '''\
# HELP yadb_node_info Always 1.
# TYPE yadb_node_info gauge
yadb_node_info{node_id="1",metadata_runtime="openraft"} 1
# HELP yadb_pool_online_slots Canonical slots currently reachable.
# TYPE yadb_pool_online_slots gauge
yadb_pool_online_slots{pool="wal-fast"} 3
yadb_pool_online_slots{pool="data-ec"} 0
'''


class ExpositionParserTest(unittest.TestCase):
    def test_types_and_labels_are_read(self):
        samples, types = alerts.parse_exposition(EXPOSITION, '127.0.0.1:1')
        self.assertEqual(types, {'yadb_node_info': 'gauge',
                                 'yadb_pool_online_slots': 'gauge'})
        self.assertEqual([s.value for s in samples], [1.0, 3.0, 0.0])
        self.assertEqual(samples[0].labels,
                         {'instance': '127.0.0.1:1', 'node_id': '1',
                          'metadata_runtime': 'openraft'})
        self.assertEqual(samples[2].labels, {'instance': '127.0.0.1:1', 'pool': 'data-ec'})

    def test_escapes_are_unescaped(self):
        text = 'yadb_pool_info{pool="a\\"b\\\\c\\nd"} 1\n'
        samples, _ = alerts.parse_exposition(text, 'x')
        self.assertEqual(samples[0].labels['pool'], 'a"b\\c\nd')

    def test_nan_and_infinities_parse(self):
        text = 'a 1\nb NaN\nc +Inf\nd -Inf\n'
        values = [s.value for s in alerts.parse_exposition(text, 'x')[0]]
        self.assertEqual(values[0], 1.0)
        self.assertNotEqual(values[1], values[1])
        self.assertEqual(values[2], float('inf'))
        self.assertEqual(values[3], float('-inf'))

    def test_a_garbage_line_is_a_failure_not_a_skipped_line(self):
        # A skipped line would look exactly like an absent family, which is the
        # bug the family-coverage check exists to catch.
        with self.assertRaises(ValueError):
            alerts.parse_exposition('yadb_node_info 1\nnot an exposition line\n', 'x')


class ScrapeTest(unittest.TestCase):
    def test_up_is_synthesised_per_target(self):
        scrape = alerts.Scrape('yadb')
        scrape.add('a', [sample('yadb_node_info', 1, instance='a')], {})
        scrape.fail('b', 'URLError: refused')
        ups = {s.labels['instance']: s.value for s in scrape.of('up')}
        self.assertEqual(ups, {'a': 1.0, 'b': 0.0})
        self.assertEqual(scrape.failed(), {'b': 'URLError: refused'})
        # Nothing else is invented for the failed target.
        self.assertEqual([s.name for s in scrape.targets['b']['samples']], [])

    def test_value_filters_on_instance_and_labels(self):
        scrape = alerts.Scrape('yadb')
        scrape.add('a', [sample('yadb_pool_online_slots', 3, instance='a', pool='wal-fast'),
                         sample('yadb_pool_online_slots', 6, instance='a', pool='data-ec')], {})
        self.assertEqual(scrape.value('yadb_pool_online_slots', 'a', pool='data-ec'), 6.0)
        self.assertIsNone(scrape.value('yadb_pool_online_slots', 'a', pool='nope'))
        self.assertIsNone(scrape.value('yadb_pool_online_slots', 'b', pool='data-ec'))


class ExpressionTest(unittest.TestCase):
    def setUp(self):
        self.samples = [
            sample('up', 1, job='yadb', instance='a'),
            sample('up', 0, job='yadb', instance='b'),
            sample('up', 1, job='other', instance='c'),
            sample('yadb_gc_refused', 1, instance='a'),
            sample('yadb_gc_refused', 0, instance='b'),
            sample('yadb_pool_online_slots', 2, instance='a', pool='wal-fast'),
            sample('yadb_pool_min_write_slots', 3, instance='a', pool='wal-fast'),
            sample('yadb_pool_online_slots', 6, instance='a', pool='data-ec'),
            sample('yadb_pool_min_write_slots', 6, instance='a', pool='data-ec'),
        ]

    def test_job_matcher_selects_only_the_scrape_job(self):
        # The quoted matcher value must be unquoted, or `up{job="yadb"}` matches
        # nothing and YadbNodeDown can never fire.
        matched = alerts.evaluate(alerts.parse_expression('up{job="yadb"} == 0'), self.samples)
        self.assertEqual([s.labels['instance'] for s in matched], ['b'])

    def test_vector_comparison_matches_on_the_full_label_set(self):
        matched = alerts.evaluate(alerts.parse_expression(
            'yadb_pool_online_slots < yadb_pool_min_write_slots'), self.samples)
        self.assertEqual([(s.labels['instance'], s.labels['pool']) for s in matched],
                         [('a', 'wal-fast')])

    def test_or_keeps_the_union_and_and_keeps_the_left_series(self):
        left = alerts.parse_expression('yadb_gc_refused > 0 or up == 0')
        self.assertEqual(len(alerts.evaluate(left, self.samples)), 2)
        both = alerts.parse_expression('up == 1 and yadb_gc_refused == 0')
        self.assertEqual(alerts.evaluate(both, self.samples), [])

    def test_scalar_comparison_returns_a_number(self):
        self.assertEqual(alerts.evaluate(alerts.parse_expression('1 == 1'), []), 1.0)
        self.assertEqual(alerts.evaluate(alerts.parse_expression('1 == 0'), []), 0.0)

    def test_rate_is_refused_rather_than_fabricated(self):
        with self.assertRaises(ValueError) as raised:
            alerts.parse_expression('rate(yadb_pool_repairs_total[5m])')
        self.assertIn('two samples', str(raised.exception))

    def test_trailing_tokens_are_rejected(self):
        with self.assertRaises(ValueError):
            alerts.parse_expression('up == 0 == 0')


class RuleSetTest(unittest.TestCase):
    def test_every_shipped_rule_is_known_and_parses(self):
        rules = alerts.load_rules()
        alerts.check_rules_known(rules)
        self.assertEqual([r['name'] for r in rules], list(alerts.EXPECTED_RULES))
        for rule in rules:
            with self.subTest(rule=rule['name']):
                alerts.parse_expression(rule['expr'])

    def test_a_new_rule_fails_the_run_instead_of_being_skipped(self):
        rules = alerts.load_rules() + [{'group': 'yadb', 'name': 'YadbNew', 'expr': 'up == 0',
                                        'for': '0s', 'severity': 'page'}]
        with self.assertRaises(AssertionError) as raised:
            alerts.check_rules_known(rules)
        self.assertIn('YadbNew', str(raised.exception))

    def test_rule_file_and_scrape_config_agree(self):
        self.assertEqual(alerts.scrape_job(), 'yadb')
        document = yaml.safe_load(alerts.ALERTS.read_text())
        self.assertEqual([g['name'] for g in document['groups']], ['yadb'])

    def test_for_durations(self):
        self.assertEqual(alerts.for_seconds('0s'), 0)
        self.assertEqual(alerts.for_seconds('2m'), 120)
        self.assertEqual(alerts.for_seconds('1h'), 3600)
        self.assertEqual(alerts.for_seconds('24h'), 86400)
        with self.assertRaises(ValueError):
            alerts.for_seconds('2 minutes')


class RuleStateTest(unittest.TestCase):
    RULE = {'group': 'yadb', 'name': 'YadbNodeDown', 'expr': 'up{job="yadb"} == 0',
            'for': '2m', 'severity': 'page'}

    def test_true_is_pending_until_for_has_elapsed(self):
        result = alerts.RuleResult(self.RULE, [sample('up', 0, instance='a')], 0)
        self.assertTrue(result.true)
        self.assertEqual(result.state, 'pending')
        held = alerts.RuleResult(self.RULE, [sample('up', 0, instance='a')], 130)
        self.assertEqual(held.state, 'firing')

    def test_false_is_inactive(self):
        self.assertEqual(alerts.RuleResult(self.RULE, [], 10_000).state, 'inactive')

    def test_summary_lists_the_instances_that_matched(self):
        result = alerts.RuleResult(self.RULE, [sample('up', 0, instance='b'),
                                               sample('up', 0, instance='a')], 0)
        self.assertEqual(result.summary(), 'a, b')


class InvertedGuardTest(unittest.TestCase):
    """Each assertion must fail on the degenerate input it exists to reject."""

    def healthy_scrape(self):
        scrape = alerts.Scrape('yadb')
        scrape.add('a', [sample('yadb_node_info', 1, instance='a'),
                         sample('yadb_pool_online_slots', 3, instance='a', pool='wal-fast'),
                         sample('yadb_pool_min_write_slots', 3, instance='a', pool='wal-fast'),
                         sample('yadb_slot_online', 1, instance='a', pool='wal-fast'),
                         sample('yadb_group_info', 1, instance='a'),
                         sample('yadb_gc_last_pass_present', 0, instance='a'),
                         sample('yadb_scrub_last_pass_present', 0, instance='a')],
                    {'yadb_node_info': 'gauge', 'yadb_pool_online_slots': 'gauge',
                     'yadb_pool_min_write_slots': 'gauge', 'yadb_slot_online': 'gauge',
                     'yadb_group_info': 'gauge', 'yadb_gc_last_pass_present': 'gauge',
                     'yadb_scrub_last_pass_present': 'gauge',
                     'yadb_gc_refused': 'gauge'})
        return scrape

    def test_a_dead_target_fails_instead_of_scraping_zero_samples(self):
        dead = alerts.Scrape('yadb')
        dead.fail('a', 'URLError: [Errno 111] Connection refused')
        with self.assertRaises(AssertionError) as raised:
            alerts.check_scrape_complete(dead, 1)
        self.assertIn('scrape failed for target(s)', str(raised.exception))
        self.assertIn('a failed scrape is not an empty exposition', str(raised.exception))

    def test_an_empty_exposition_fails_the_coverage_check(self):
        with self.assertRaises(AssertionError) as raised:
            alerts.check_family_coverage(alerts.Scrape('yadb'))
        self.assertIn('no family is declared', str(raised.exception))

    def test_an_empty_sample_set_fails_the_healthy_assertion(self):
        # Every rule is false over no samples, so without this guard "0 firing"
        # would be the verdict for a cluster that answered nothing at all.
        with self.assertRaises(AssertionError) as raised:
            alerts.check_healthy([], alerts.Scrape('yadb'))
        self.assertIn('has no samples', str(raised.exception))
        self.assertIn('would be vacuous', str(raised.exception))

    def test_a_declared_family_absent_with_its_gate_clear_fails(self):
        scrape = self.healthy_scrape()
        alerts.check_family_coverage(scrape)  # yadb_gc_refused absent, gate 0: explained
        scrape.targets['a']['samples'] = [s for s in scrape.targets['a']['samples']
                                          if s.name != 'yadb_pool_online_slots']
        with self.assertRaises(AssertionError) as raised:
            alerts.check_family_coverage(scrape)
        self.assertIn('yadb_pool_online_slots', str(raised.exception))

    def test_a_pass_only_family_absent_with_its_gate_set_fails(self):
        scrape = self.healthy_scrape()
        for sample_ in scrape.targets['a']['samples']:
            if sample_.name == 'yadb_gc_last_pass_present':
                sample_.value = 1.0
        with self.assertRaises(AssertionError) as raised:
            alerts.check_family_coverage(scrape)
        self.assertIn('yadb_gc_refused', str(raised.exception))

    def test_a_family_present_without_a_declaration_fails(self):
        scrape = self.healthy_scrape()
        scrape.targets['a']['samples'].append(sample('yadb_mystery', 1, instance='a'))
        with self.assertRaises(AssertionError) as raised:
            alerts.check_family_coverage(scrape)
        self.assertIn('undeclared families: yadb_mystery', str(raised.exception))

    def test_a_target_without_node_info_fails_the_healthy_assertion(self):
        scrape = self.healthy_scrape()
        scrape.targets['a']['samples'] = [s for s in scrape.targets['a']['samples']
                                          if s.name != 'yadb_node_info']
        with self.assertRaises(AssertionError) as raised:
            alerts.check_healthy([], scrape)
        self.assertIn('yadb_node_info has no samples', str(raised.exception))

    def test_a_firing_rule_fails_the_healthy_assertion(self):
        scrape = self.healthy_scrape()
        rules = [{'group': 'yadb', 'name': 'YadbNodeDown', 'expr': 'up{job="yadb"} == 0',
                  'for': '0s', 'severity': 'page'}]
        samples = scrape.samples() + [sample('up', 0, job='yadb', instance='a')]
        results = alerts.evaluate_rules(rules, samples)
        self.assertEqual(results[0].state, 'firing')
        with self.assertRaises(AssertionError) as raised:
            alerts.check_healthy(results, scrape)
        self.assertIn('rules firing on a healthy cluster', str(raised.exception))

    def test_an_undocumented_true_expression_fails(self):
        scrape = self.healthy_scrape()
        rules = [{'group': 'yadb', 'name': 'YadbPoolNoOnlineSlots',
                  'expr': 'yadb_pool_online_slots == 3', 'for': '1h', 'severity': 'page'}]
        results = alerts.evaluate_rules(rules, scrape.samples())
        self.assertEqual(results[0].state, 'pending')
        with self.assertRaises(AssertionError) as raised:
            alerts.check_healthy(results, scrape)
        self.assertIn('no documented mechanism', str(raised.exception))

    def test_a_documented_exception_that_stops_reproducing_fails(self):
        # The list records a defect; when the defect is fixed the entry must go.
        scrape = self.healthy_scrape()
        rules = [{'group': 'yadb', 'name': 'YadbGcNoPass',
                  'expr': 'yadb_gc_last_pass_present == 1', 'for': '1h', 'severity': 'warning'}]
        results = alerts.evaluate_rules(rules, scrape.samples())
        self.assertFalse(results[0].true)
        with self.assertRaises(AssertionError) as raised:
            alerts.check_healthy(results, scrape)
        self.assertIn('no longer true', str(raised.exception))


class EvidenceTest(unittest.TestCase):
    def test_every_rule_has_a_measured_note(self):
        scrape = alerts.Scrape('yadb')
        scrape.add('127.0.0.1:1', [sample('yadb_node_info', 1, instance='127.0.0.1:1'),
                                   sample('yadb_pool_online_slots', 3,
                                          instance='127.0.0.1:1', pool='wal-fast'),
                                   sample('yadb_pool_min_write_slots', 3,
                                          instance='127.0.0.1:1', pool='wal-fast'),
                                   sample('yadb_slot_online', 1,
                                          instance='127.0.0.1:1', pool='wal-fast'),
                                   sample('yadb_group_error', 0, instance='127.0.0.1:1',
                                          group='1'),
                                   sample('yadb_group_info', 1, instance='127.0.0.1:1',
                                          group='1'),
                                   sample('yadb_gc_last_pass_present', 0,
                                          instance='127.0.0.1:1'),
                                   sample('yadb_scrub_last_pass_present', 0,
                                          instance='127.0.0.1:1')], {})
        notes = alerts.evidence(scrape, 0)
        self.assertEqual(set(notes), set(alerts.EXPECTED_RULES))
        for name, note in notes.items():
            with self.subTest(rule=name):
                self.assertTrue(note.strip())
        self.assertIn('wal-fast', notes['YadbPoolNoOnlineSlots'])
        self.assertIn('absent', notes['YadbGcRefused'])


if __name__ == '__main__':
    unittest.main()
