"""Tests for computed RecommendationInputs fields (regulatory, usage, complexity, layer)."""
import unittest

from src.accelerator.models import MetricInstance
from src.accelerator.pipeline import (
    GOVERNED_PATTERNS,
    _compute_complexity,
    _compute_regulatory_sensitivity,
    _compute_usage_frequency,
    _detect_layer_violation,
    cluster_metrics,
    run_analysis,
)


def _m(metric_id, *, regulatory_tag=None, usage_count=None,
        filters=None, source_objects=None, expression_signature="sum(x)",
        metric_name="KPI", report_id="r1"):
    """Convenience factory for MetricInstance."""
    return MetricInstance(
        metric_id=metric_id,
        report_id=report_id,
        dataset_id="d1",
        metric_name=metric_name,
        expression_signature=expression_signature,
        grain="day",
        filters=filters if filters is not None else [],
        join_path_signature="a->b",
        source_objects=source_objects if source_objects is not None else ["raw.table"],
        regulatory_tag=regulatory_tag,
        usage_count=usage_count,
    )


class TestRegulatorySenitivity(unittest.TestCase):
    def test_high_tag_returns_90(self):
        self.assertEqual(_compute_regulatory_sensitivity([_m("m1", regulatory_tag="high")]), 90.0)

    def test_medium_tag_returns_60(self):
        self.assertEqual(_compute_regulatory_sensitivity([_m("m1", regulatory_tag="medium")]), 60.0)

    def test_low_tag_returns_30(self):
        self.assertEqual(_compute_regulatory_sensitivity([_m("m1", regulatory_tag="low")]), 30.0)

    def test_none_tag_returns_10(self):
        self.assertEqual(_compute_regulatory_sensitivity([_m("m1", regulatory_tag="none")]), 10.0)

    def test_missing_tag_returns_neutral_50(self):
        self.assertEqual(_compute_regulatory_sensitivity([_m("m1")]), 50.0)

    def test_cluster_uses_max_of_members(self):
        members = [_m("m1", regulatory_tag="low"), _m("m2", regulatory_tag="high")]
        self.assertEqual(_compute_regulatory_sensitivity(members), 90.0)

    def test_tag_case_insensitive(self):
        self.assertEqual(_compute_regulatory_sensitivity([_m("m1", regulatory_tag="HIGH")]), 90.0)
        self.assertEqual(_compute_regulatory_sensitivity([_m("m1", regulatory_tag="Medium")]), 60.0)


class TestUsageFrequency(unittest.TestCase):
    def _all(self, *metrics):
        return list(metrics)

    def test_zero_usage_returns_low_score(self):
        m1 = _m("m1", usage_count=0)
        m2 = _m("m2", usage_count=100)
        score = _compute_usage_frequency([m1], self._all(m1, m2))
        self.assertEqual(score, 0.0)

    def test_max_usage_returns_100(self):
        m1 = _m("m1", usage_count=100)
        score = _compute_usage_frequency([m1], self._all(m1))
        self.assertEqual(score, 100.0)

    def test_half_usage_returns_50(self):
        m1 = _m("m1", usage_count=50)
        m2 = _m("m2", usage_count=100)
        score = _compute_usage_frequency([m1], self._all(m1, m2))
        self.assertEqual(score, 50.0)

    def test_no_usage_data_returns_neutral_50(self):
        m1 = _m("m1")   # usage_count=None
        m2 = _m("m2")
        score = _compute_usage_frequency([m1], self._all(m1, m2))
        self.assertEqual(score, 50.0)

    def test_cluster_usage_sums_members(self):
        m1 = _m("m1", usage_count=40)
        m2 = _m("m2", usage_count=40)
        m3 = _m("m3", usage_count=100)
        # cluster = [m1, m2], total = 80; max = 100 → 80%
        score = _compute_usage_frequency([m1, m2], self._all(m1, m2, m3))
        self.assertEqual(score, 80.0)


class TestComplexity(unittest.TestCase):
    def test_no_filters_no_extra_sources_simple_formula(self):
        m = _m("m1", filters=[], source_objects=["raw.t"], expression_signature="sum(x)")
        score = _compute_complexity([m])
        # 0 filters → 0; 1 source → 10; len("sum(x)")/5 = 1.2 → ~1.2 total ≈ 11.2
        self.assertLess(score, 30.0)

    def test_many_filters_increases_score(self):
        low  = _compute_complexity([_m("m1", filters=[])])
        high = _compute_complexity([_m("m2", filters=["a=1", "b=2", "c=3"])])
        self.assertGreater(high, low)

    def test_many_sources_increases_score(self):
        low  = _compute_complexity([_m("m1", source_objects=["t1"])])
        high = _compute_complexity([_m("m2", source_objects=["t1", "t2", "t3", "t4"])])
        self.assertGreater(high, low)

    def test_long_formula_increases_score(self):
        low  = _compute_complexity([_m("m1", expression_signature="sum(x)")])
        high = _compute_complexity([_m("m2", expression_signature="sum(case when status='active' and region in ('A','B','C') then amount else 0 end)")])
        self.assertGreater(high, low)

    def test_score_capped_at_100(self):
        m = _m("m1",
               filters=["a=1"] * 20,
               source_objects=["t"] * 20,
               expression_signature="x" * 200)
        self.assertLessEqual(_compute_complexity([m]), 100.0)

    def test_takes_max_across_members(self):
        simple  = _m("m1", filters=[])
        complex_ = _m("m2", filters=["a=1", "b=2", "c=3"], source_objects=["t1", "t2", "t3"])
        self.assertEqual(
            _compute_complexity([simple, complex_]),
            _compute_complexity([complex_])
        )


class TestLayerViolation(unittest.TestCase):
    def test_raw_source_triggers_violation(self):
        m = _m("m1", source_objects=["raw.sales"])
        self.assertTrue(_detect_layer_violation([m]))

    def test_semantic_view_pattern_clears_violation(self):
        m = _m("m1", source_objects=["curated.sales_semantic_view"])
        self.assertFalse(_detect_layer_violation([m]))

    def test_vw_prefix_clears_violation(self):
        m = _m("m1", source_objects=["vw_sales"])
        self.assertFalse(_detect_layer_violation([m]))

    def test_curated_prefix_clears_violation(self):
        m = _m("m1", source_objects=["curated.revenue"])
        self.assertFalse(_detect_layer_violation([m]))

    def test_gold_prefix_clears_violation(self):
        m = _m("m1", source_objects=["gold.kpi_nav"])
        self.assertFalse(_detect_layer_violation([m]))

    def test_semantic_view_space_variant_clears_violation(self):
        m = _m("m1", source_objects=["semantic view.nav"])
        self.assertFalse(_detect_layer_violation([m]))

    def test_mixed_sources_one_governed_clears_violation(self):
        # At least one governed source → no violation
        m = _m("m1", source_objects=["raw.sales", "curated.enriched"])
        self.assertFalse(_detect_layer_violation([m]))

    def test_empty_sources_no_violation(self):
        m = _m("m1", source_objects=[])
        self.assertFalse(_detect_layer_violation([m]))

    def test_all_patterns_present_in_constant(self):
        for pattern in ["semantic_view", "semantic view", "vw_", "curated.", "gold."]:
            self.assertIn(pattern, GOVERNED_PATTERNS)


class TestBackwardCompatibility(unittest.TestCase):
    """Existing payloads without new fields must produce valid results."""

    def _legacy_metric(self, metric_id, report_id, metric_name, expression_signature,
                       grain, filters, join_path_signature, source_objects):
        return MetricInstance(
            metric_id=metric_id,
            report_id=report_id,
            dataset_id="d1",
            metric_name=metric_name,
            expression_signature=expression_signature,
            grain=grain,
            filters=filters,
            join_path_signature=join_path_signature,
            source_objects=source_objects,
            # regulatory_tag and usage_count intentionally omitted
        )

    def test_legacy_payload_runs_without_error(self):
        metrics = [
            self._legacy_metric("m1", "r1", "NAV", "sum(nav)", "fund_date",
                                ["trade_date=:as_of"], "fund->positions", ["raw.positions"]),
            self._legacy_metric("m2", "r2", "NAV", "sum(nav)", "fund_date",
                                ["settle_date=:as_of"], "fund->positions", ["raw.positions"]),
        ]
        result = run_analysis(metrics, use_advanced_clustering=False)
        self.assertIn("clusters", result)
        self.assertIn("drift_findings", result)
        self.assertIn("recommendations", result)

    def test_legacy_payload_regulatory_defaults_to_neutral(self):
        m = self._legacy_metric("m1", "r1", "X", "sum(x)", "day", [], "a->b", ["raw.t"])
        self.assertEqual(_compute_regulatory_sensitivity([m]), 50.0)

    def test_legacy_payload_usage_defaults_to_neutral(self):
        m = self._legacy_metric("m1", "r1", "X", "sum(x)", "day", [], "a->b", ["raw.t"])
        self.assertEqual(_compute_usage_frequency([m], [m]), 50.0)


class TestRetireActionWithLowUsage(unittest.TestCase):
    """Low usage relative to dataset max on a duplicated cluster should trigger RETIRE.

    Conditions required for RETIRE (see choose_action):
      - layer_violation=False  → use a governed source (vw_sales)
      - drift_severity < 80    → identical metrics, no drift
      - duplication_count >= 2 → 3 cluster members (duplication_count = 2)
      - duplication_count < 3  → keeps CONSOLIDATE from firing
      - usage_frequency < 25   → cluster usage=15 out of max=100 → 15%
    """

    def test_low_usage_duplication_triggers_retire(self):
        from src.accelerator.models import MetricCluster, ActionType
        from src.accelerator.pipeline import build_recommendations, detect_drift

        # Cluster members: very low usage, governed source, identical formula
        cluster_metrics = [
            MetricInstance("m1", "r1", "d1", "Sales", "sum(sales)", "day", [],
                           "a->b", ["vw_sales"], usage_count=5),
            MetricInstance("m2", "r2", "d2", "Sales", "sum(sales)", "day", [],
                           "a->b", ["vw_sales"], usage_count=5),
            MetricInstance("m3", "r3", "d3", "Sales", "sum(sales)", "day", [],
                           "a->b", ["vw_sales"], usage_count=5),
        ]
        # Anchor metric with high usage drives max_usage to 100
        anchor = MetricInstance("a1", "r4", "d4", "Revenue", "sum(rev)", "day", [],
                                "a->b", ["vw_revenue"], usage_count=100)
        all_metrics = cluster_metrics + [anchor]

        lookup  = {m.metric_id: m for m in cluster_metrics}
        cluster = MetricCluster("CL-0001", ["m1", "m2", "m3"], 0.95)
        findings = detect_drift(cluster, lookup)
        recs = build_recommendations([cluster], findings, lookup, all_metrics)

        # usage_frequency = (5+5+5)/100 * 100 = 15.0 < 25, duplication_count=2 >= 2
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].action_type, ActionType.RETIRE)


if __name__ == "__main__":
    unittest.main()
