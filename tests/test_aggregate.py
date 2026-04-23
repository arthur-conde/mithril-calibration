"""Tests for scripts/aggregate.py. Stdlib only."""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import aggregate  # noqa: E402


README_TEMPLATE = """# gorgon-calibration

Preamble that must not change.

<!-- contributors:start -->
_(No contributors yet — be the first!)_
<!-- contributors:end -->

Footer that must not change either.
"""


def _write(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _samwise_payload(rates=None, phase_rates=None, slot_caps=None, *, submitter="alice", opt_out=False, extra=None):
    p = {
        "schemaVersion": 1,
        "module": "samwise",
        "exportedAt": "2026-04-22T00:00:00Z",
        "submitter": submitter,
        "attributionOptOut": opt_out,
        "rates": rates or {},
        "phaseRates": phase_rates or {},
        "slotCapRates": slot_caps or {},
    }
    if extra:
        p.update(extra)
    return p


def _arwen_payload(item=None, sig=None, npc=None, kw=None, *, submitter="alice", opt_out=False):
    return {
        "schemaVersion": 2,
        "module": "arwen",
        "exportedAt": "2026-04-22T00:00:00Z",
        "submitter": submitter,
        "attributionOptOut": opt_out,
        "itemRates": item or {},
        "signatureRates": sig or {},
        "npcRates": npc or {},
        "keywordRates": kw or {},
    }


def _smaug_payload(absolute=None, ratio=None, *, submitter="alice", opt_out=False):
    return {
        "schemaVersion": 1,
        "module": "smaug",
        "exportedAt": "2026-04-22T00:00:00Z",
        "submitter": submitter,
        "attributionOptOut": opt_out,
        "absoluteRates": absolute or {},
        "ratioRates": ratio or {},
    }


class AggregateTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.contrib = self.root / "contributions"
        self.aggregated = self.root / "aggregated"
        (self.contrib / "samwise").mkdir(parents=True)
        (self.contrib / "arwen").mkdir(parents=True)
        (self.contrib / "smaug").mkdir(parents=True)
        self.aggregated.mkdir(parents=True)
        self.readme = self.root / "README.md"
        self.readme.write_text(README_TEMPLATE, encoding="utf-8")

    def tearDown(self):
        self._tmp.cleanup()

    def run_aggregate(self, now=None):
        aggregate.run_aggregate(
            contrib_root=self.contrib,
            aggregated_root=self.aggregated,
            readme_path=self.readme,
            now=now or datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc),
        )

    def read_aggregated(self, module):
        return json.loads((self.aggregated / f"{module}.json").read_text(encoding="utf-8"))


class TestMergeMath(AggregateTestBase):
    def test_weighted_mean_samwise_rates(self):
        _write(self.contrib / "samwise" / "1.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 3600, "sampleCount": 10, "minSeconds": 3500, "maxSeconds": 3700}},
        ))
        _write(self.contrib / "samwise" / "2.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 3700, "sampleCount": 5, "minSeconds": 3400, "maxSeconds": 3800}},
        ))
        self.run_aggregate()
        agg = self.read_aggregated("samwise")
        carrot = agg["rates"]["Carrot"]
        expected_avg = (3600 * 10 + 3700 * 5) / 15
        self.assertAlmostEqual(carrot["avgSeconds"], expected_avg)
        self.assertEqual(carrot["sampleCount"], 15)
        self.assertEqual(carrot["minSeconds"], 3400)
        self.assertEqual(carrot["maxSeconds"], 3800)

    def test_weighted_mean_arwen_rates(self):
        _write(self.contrib / "arwen" / "1.json", _arwen_payload(
            sig={"NPC_Makara|Rings": {"rate": 0.10, "sampleCount": 10, "minRate": 0.05, "maxRate": 0.20}},
        ))
        _write(self.contrib / "arwen" / "2.json", _arwen_payload(
            sig={"NPC_Makara|Rings": {"rate": 0.20, "sampleCount": 30, "minRate": 0.08, "maxRate": 0.30}},
        ))
        self.run_aggregate()
        agg = self.read_aggregated("arwen")
        rate = agg["signatureRates"]["NPC_Makara|Rings"]
        expected = (0.10 * 10 + 0.20 * 30) / 40
        self.assertAlmostEqual(rate["rate"], expected)
        self.assertEqual(rate["sampleCount"], 40)
        self.assertAlmostEqual(rate["minRate"], 0.05)
        self.assertAlmostEqual(rate["maxRate"], 0.30)

    def test_slot_cap_is_max_not_mean(self):
        _write(self.contrib / "samwise" / "1.json", _samwise_payload(
            slot_caps={"Lily": {"observedMax": 8, "sampleCount": 3}},
        ))
        _write(self.contrib / "samwise" / "2.json", _samwise_payload(
            slot_caps={"Lily": {"observedMax": 10, "sampleCount": 5}},
        ))
        self.run_aggregate()
        agg = self.read_aggregated("samwise")
        self.assertEqual(agg["slotCapRates"]["Lily"]["observedMax"], 10)
        self.assertEqual(agg["slotCapRates"]["Lily"]["sampleCount"], 8)


class TestSmaugMergeMath(AggregateTestBase):
    ABS_KEY = "NPC_Yetta|BottleOfWater|Neutral|45+"
    RATIO_KEY = "NPC_Yetta|Augment|Friends|5-14"

    def test_smaug_absolute_rates_weighted_mean(self):
        _write(self.contrib / "smaug" / "1.json", _smaug_payload(
            absolute={self.ABS_KEY: {"avgPrice": 10, "sampleCount": 4, "minPrice": 8, "maxPrice": 12}},
        ))
        _write(self.contrib / "smaug" / "2.json", _smaug_payload(
            absolute={self.ABS_KEY: {"avgPrice": 14, "sampleCount": 6, "minPrice": 11, "maxPrice": 18}},
        ))
        self.run_aggregate()
        agg = self.read_aggregated("smaug")
        entry = agg["absoluteRates"][self.ABS_KEY]
        expected = (10 * 4 + 14 * 6) / 10
        self.assertAlmostEqual(entry["avgPrice"], expected)
        self.assertEqual(entry["sampleCount"], 10)
        self.assertEqual(entry["minPrice"], 8)
        self.assertEqual(entry["maxPrice"], 18)

    def test_smaug_ratio_rates_weighted_mean(self):
        _write(self.contrib / "smaug" / "1.json", _smaug_payload(
            ratio={self.RATIO_KEY: {"avgRatio": 0.40, "sampleCount": 12, "minRatio": 0.20, "maxRatio": 0.60}},
        ))
        _write(self.contrib / "smaug" / "2.json", _smaug_payload(
            ratio={self.RATIO_KEY: {"avgRatio": 0.85, "sampleCount": 8, "minRatio": 0.70, "maxRatio": 1.00}},
        ))
        self.run_aggregate()
        agg = self.read_aggregated("smaug")
        entry = agg["ratioRates"][self.RATIO_KEY]
        expected = (0.40 * 12 + 0.85 * 8) / 20
        self.assertAlmostEqual(entry["avgRatio"], expected)
        self.assertEqual(entry["sampleCount"], 20)
        self.assertAlmostEqual(entry["minRatio"], 0.20)
        self.assertAlmostEqual(entry["maxRatio"], 1.00)

    def test_smaug_disjoint_keys_copied_through(self):
        unique_a = "NPC_Yetta|Carrot|Neutral|0-4"
        unique_b = "NPC_Yetta|Onion|Neutral|45+"
        _write(self.contrib / "smaug" / "1.json", _smaug_payload(
            absolute={unique_a: {"avgPrice": 5, "sampleCount": 2, "minPrice": 5, "maxPrice": 5}},
        ))
        _write(self.contrib / "smaug" / "2.json", _smaug_payload(
            absolute={unique_b: {"avgPrice": 7, "sampleCount": 3, "minPrice": 7, "maxPrice": 7}},
        ))
        self.run_aggregate()
        agg = self.read_aggregated("smaug")
        self.assertEqual(agg["absoluteRates"][unique_a]["sampleCount"], 2)
        self.assertEqual(agg["absoluteRates"][unique_b]["sampleCount"], 3)
        self.assertAlmostEqual(agg["absoluteRates"][unique_a]["avgPrice"], 5)
        self.assertAlmostEqual(agg["absoluteRates"][unique_b]["avgPrice"], 7)

    def test_smaug_dicts_dont_cross(self):
        shared = "NPC_Yetta|GloveAugment|Friends|45+"
        _write(self.contrib / "smaug" / "1.json", _smaug_payload(
            absolute={shared: {"avgPrice": 100, "sampleCount": 2, "minPrice": 100, "maxPrice": 100}},
        ))
        _write(self.contrib / "smaug" / "2.json", _smaug_payload(
            ratio={shared: {"avgRatio": 0.5, "sampleCount": 3, "minRatio": 0.5, "maxRatio": 0.5}},
        ))
        self.run_aggregate()
        agg = self.read_aggregated("smaug")
        self.assertIn(shared, agg["absoluteRates"])
        self.assertIn(shared, agg["ratioRates"])
        self.assertEqual(agg["absoluteRates"][shared]["sampleCount"], 2)
        self.assertEqual(agg["ratioRates"][shared]["sampleCount"], 3)
        self.assertAlmostEqual(agg["absoluteRates"][shared]["avgPrice"], 100)
        self.assertAlmostEqual(agg["ratioRates"][shared]["avgRatio"], 0.5)


class TestSmaugValidation(AggregateTestBase):
    def test_wrong_schema_version_rejected(self):
        bad = _smaug_payload(
            absolute={"NPC_X|Y|Neutral|0-4": {"avgPrice": 1, "sampleCount": 1, "minPrice": 1, "maxPrice": 1}},
        )
        bad["schemaVersion"] = 2
        _write(self.contrib / "smaug" / "1.json", bad)
        with self.assertLogs(aggregate.log, level="WARNING") as cm:
            self.run_aggregate()
        self.assertTrue(any("1.json" in m and "schemaVersion" in m for m in cm.output))
        agg = self.read_aggregated("smaug")
        self.assertEqual(agg["absoluteRates"], {})
        self.assertEqual(agg["ratioRates"], {})


class TestValidation(AggregateTestBase):
    def test_schema_version_mismatch_rejected(self):
        bad = _arwen_payload(sig={"X|Y": {"rate": 0.5, "sampleCount": 1, "minRate": 0.5, "maxRate": 0.5}})
        bad["schemaVersion"] = 1
        _write(self.contrib / "arwen" / "1.json", bad)
        good = _arwen_payload(sig={"A|B": {"rate": 0.3, "sampleCount": 2, "minRate": 0.3, "maxRate": 0.3}})
        _write(self.contrib / "arwen" / "2.json", good)
        with self.assertLogs(aggregate.log, level="WARNING") as cm:
            self.run_aggregate()
        self.assertTrue(any("1.json" in m and "schemaVersion" in m for m in cm.output))
        agg = self.read_aggregated("arwen")
        self.assertNotIn("X|Y", agg["signatureRates"])
        self.assertIn("A|B", agg["signatureRates"])

    def test_module_mismatch_rejected(self):
        payload = _samwise_payload()
        payload["module"] = "arwen"
        _write(self.contrib / "samwise" / "1.json", payload)
        with self.assertLogs(aggregate.log, level="WARNING") as cm:
            self.run_aggregate()
        self.assertTrue(any("module" in m.lower() for m in cm.output))

    def test_unknown_fields_ignored(self):
        payload = _samwise_payload(
            rates={"Onion": {"avgSeconds": 100, "sampleCount": 1, "minSeconds": 100, "maxSeconds": 100}},
            extra={"futureField": "x", "anotherFuture": 42},
        )
        _write(self.contrib / "samwise" / "1.json", payload)
        with self.assertLogs(aggregate.log, level="INFO") as cm:
            self.run_aggregate()
        self.assertTrue(any("futureField" in m for m in cm.output))
        agg = self.read_aggregated("samwise")
        self.assertIn("Onion", agg["rates"])


class TestAttribution(AggregateTestBase):
    def test_opt_out_excluded_from_readme(self):
        _write(self.contrib / "samwise" / "1.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 100, "sampleCount": 5, "minSeconds": 100, "maxSeconds": 100}},
            submitter="alice", opt_out=False,
        ))
        _write(self.contrib / "samwise" / "2.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 200, "sampleCount": 5, "minSeconds": 200, "maxSeconds": 200}},
            submitter="bob", opt_out=True,
        ))
        self.run_aggregate()
        agg = self.read_aggregated("samwise")
        self.assertEqual(agg["rates"]["Carrot"]["sampleCount"], 10)
        text = self.readme.read_text(encoding="utf-8")
        self.assertIn("alice", text)
        self.assertNotIn("bob", text)

    def test_readme_preservation(self):
        _write(self.contrib / "samwise" / "1.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 100, "sampleCount": 1, "minSeconds": 100, "maxSeconds": 100}},
            submitter="alice",
        ))
        original = self.readme.read_text(encoding="utf-8")
        self.run_aggregate()
        new = self.readme.read_text(encoding="utf-8")
        before_new = new.split(aggregate.CONTRIBUTORS_START)[0]
        before_old = original.split(aggregate.CONTRIBUTORS_START)[0]
        self.assertEqual(before_new, before_old)
        after_new = new.split(aggregate.CONTRIBUTORS_END)[1]
        after_old = original.split(aggregate.CONTRIBUTORS_END)[1]
        self.assertEqual(after_new, after_old)


class TestIdempotence(AggregateTestBase):
    def test_idempotence(self):
        _write(self.contrib / "samwise" / "1.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 100, "sampleCount": 1, "minSeconds": 100, "maxSeconds": 100}},
            submitter="alice",
        ))
        self.run_aggregate(now=datetime(2026, 4, 23, 12, 0, 0, tzinfo=timezone.utc))
        agg_bytes_1 = (self.aggregated / "samwise.json").read_bytes()
        readme_bytes_1 = self.readme.read_bytes()
        self.run_aggregate(now=datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc))
        agg_bytes_2 = (self.aggregated / "samwise.json").read_bytes()
        readme_bytes_2 = self.readme.read_bytes()
        self.assertEqual(agg_bytes_1, agg_bytes_2)
        self.assertEqual(readme_bytes_1, readme_bytes_2)

    def test_aggregated_at_preserved_on_no_op(self):
        _write(self.contrib / "samwise" / "1.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 100, "sampleCount": 1, "minSeconds": 100, "maxSeconds": 100}},
            submitter="alice",
        ))
        self.run_aggregate(now=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
        first = self.read_aggregated("samwise")["aggregatedAt"]
        self.run_aggregate(now=datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc))
        second = self.read_aggregated("samwise")["aggregatedAt"]
        self.assertEqual(first, second)

    def test_aggregated_at_updated_on_change(self):
        _write(self.contrib / "samwise" / "1.json", _samwise_payload(
            rates={"Carrot": {"avgSeconds": 100, "sampleCount": 1, "minSeconds": 100, "maxSeconds": 100}},
            submitter="alice",
        ))
        self.run_aggregate(now=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
        first = self.read_aggregated("samwise")["aggregatedAt"]
        _write(self.contrib / "samwise" / "2.json", _samwise_payload(
            rates={"Onion": {"avgSeconds": 50, "sampleCount": 1, "minSeconds": 50, "maxSeconds": 50}},
            submitter="bob",
        ))
        self.run_aggregate(now=datetime(2026, 4, 24, 12, 0, 0, tzinfo=timezone.utc))
        second = self.read_aggregated("samwise")["aggregatedAt"]
        self.assertNotEqual(first, second)


class TestIngestIssue(AggregateTestBase):
    def _make_event(self, *, number, login, body, labels, is_pr=False, title="[samwise-contribution] test"):
        key = "pull_request" if is_pr else "issue"
        return {
            key: {
                "number": number,
                "user": {"login": login},
                "body": body,
                "labels": [{"name": l} for l in labels],
                "title": title,
            }
        }

    def test_ingest_issue_overwrites_submitter(self):
        claimed = _samwise_payload(
            rates={"Carrot": {"avgSeconds": 100, "sampleCount": 1, "minSeconds": 100, "maxSeconds": 100}},
            submitter="evil_impersonator",
        )
        body = "Here is my data:\n\n```json\n" + json.dumps(claimed, indent=2) + "\n```\n"
        event = self._make_event(number=42, login="real_user", body=body, labels=["contribution", "samwise"])
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(event), encoding="utf-8")

        dest = aggregate.ingest_issue(event_path, contrib_root=self.contrib)

        self.assertEqual(dest, self.contrib / "samwise" / "42.json")
        stored = json.loads(dest.read_text(encoding="utf-8"))
        self.assertEqual(stored["submitter"], "real_user")
        self.assertNotEqual(stored["submitter"], "evil_impersonator")

    def test_ingest_issue_opt_out_checkbox(self):
        payload = _samwise_payload(
            rates={"Carrot": {"avgSeconds": 100, "sampleCount": 1, "minSeconds": 100, "maxSeconds": 100}},
        )
        body = (
            "```json\n" + json.dumps(payload) + "\n```\n\n"
            "### Attribution\n\n- [x] Don't list me in the README contributors block\n"
        )
        event = self._make_event(number=7, login="alice", body=body, labels=["contribution", "samwise"])
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(event), encoding="utf-8")
        aggregate.ingest_issue(event_path, contrib_root=self.contrib)
        stored = json.loads((self.contrib / "samwise" / "7.json").read_text(encoding="utf-8"))
        self.assertTrue(stored["attributionOptOut"])

    def test_ingest_smaug_issue_by_label(self):
        payload = _smaug_payload(
            absolute={"NPC_Yetta|BottleOfWater|Neutral|45+": {
                "avgPrice": 11, "sampleCount": 4, "minPrice": 11, "maxPrice": 11,
            }},
            submitter="evil_impersonator",
        )
        body = "```json\n" + json.dumps(payload) + "\n```"
        event = self._make_event(
            number=99, login="real_user", body=body,
            labels=["contribution", "smaug"],
            title="[smaug-contribution] vendor prices from Yetta",
        )
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(event), encoding="utf-8")

        dest = aggregate.ingest_issue(event_path, contrib_root=self.contrib)
        self.assertEqual(dest, self.contrib / "smaug" / "99.json")
        stored = json.loads(dest.read_text(encoding="utf-8"))
        self.assertEqual(stored["submitter"], "real_user")

    def test_ingest_issue_validation_failure_exit_code(self):
        bad = _samwise_payload()
        bad["schemaVersion"] = 999
        body = "```json\n" + json.dumps(bad) + "\n```"
        event = self._make_event(number=9, login="alice", body=body, labels=["contribution", "samwise"])
        event_path = self.root / "event.json"
        event_path.write_text(json.dumps(event), encoding="utf-8")

        rc = aggregate.main([
            "--ingest-issue", str(event_path),
            "--contributions-root", str(self.contrib),
            "--aggregated-root", str(self.aggregated),
            "--readme", str(self.readme),
        ])
        self.assertEqual(rc, aggregate.EXIT_WRONG_SCHEMA_VERSION)
        self.assertFalse((self.contrib / "samwise" / "9.json").exists())


if __name__ == "__main__":
    unittest.main()
