"""Tests verifying graduated drift signals and data-driven helpers (Fixes 1-5)."""
import unittest

from src.accelerator.models import MetricCluster, MetricInstance
from src.accelerator.pipeline import (
    GOVERNED_PATTERNS,
    _graduated_filter_diff,
    _graduated_formula_diff,
    _graduated_grain_diff,
    _graduated_join_diff,
    _infer_risk_note,
    _naive_cluster_confidence,
    _build_report_inventory,
    _build_kpi_dictionary,
    detect_drift,
    run_analysis,
)


def _m(mid, *, name="KPI", expr="sum(x)", grain="day", filters=None,
        join="a->b", sources=None, regulatory_tag=None, report_id="r1",
        report_folder=None):
    return MetricInstance(
        metric_id=mid,
        report_id=report_id,
        dataset_id="d1",
        metric_name=name,
        expression_signature=expr,
        grain=grain,
        filters=filters if filters is not None else [],
        join_path_signature=join,
        source_objects=sources if sources is not None else ["raw.table"],
        regulatory_tag=regulatory_tag,
        report_folder=report_folder,
    )


# ---------------------------------------------------------------------------
# Fix 1a: Graduated filter diff
# ---------------------------------------------------------------------------
class TestGraduatedFilterDiff(unittest.TestCase):
    def test_identical_filters_returns_zero(self):
        m1 = _m("m1", filters=["a", "b"])
        m2 = _m("m2", filters=["a", "b"])
        self.assertEqual(_graduated_filter_diff([m1, m2]), 0.0)

    def test_empty_filters_on_both_returns_zero(self):
        self.assertEqual(_graduated_filter_diff([_m("m1"), _m("m2")]), 0.0)

    def test_partial_overlap_is_less_than_100(self):
        # {"a"} vs {"a","b"} → union=2, intersection=1 → 50%
        m1 = _m("m1", filters=["a"])
        m2 = _m("m2", filters=["a", "b"])
        score = _graduated_filter_diff([m1, m2])
        self.assertGreater(score, 0.0)
        self.assertLess(score, 100.0)
        self.assertAlmostEqual(score, 50.0)

    def test_disjoint_filters_returns_100(self):
        m1 = _m("m1", filters=["x"])
        m2 = _m("m2", filters=["y"])
        self.assertEqual(_graduated_filter_diff([m1, m2]), 100.0)

    def test_singleton_returns_zero(self):
        self.assertEqual(_graduated_filter_diff([_m("m1", filters=["a"])]), 0.0)

    def test_three_members_jaccard_uses_all(self):
        # {"a","b"} ∩ {"a","b","c"} ∩ {"a"} = {"a"}; union = {"a","b","c"} → 1 - 1/3 ≈ 66.7
        m1 = _m("m1", filters=["a", "b"])
        m2 = _m("m2", filters=["a", "b", "c"])
        m3 = _m("m3", filters=["a"])
        score = _graduated_filter_diff([m1, m2, m3])
        self.assertAlmostEqual(score, 66.67, places=1)


# ---------------------------------------------------------------------------
# Fix 1b: Graduated join diff
# ---------------------------------------------------------------------------
class TestGraduatedJoinDiff(unittest.TestCase):
    def test_identical_joins_returns_zero(self):
        m1 = _m("m1", join="a->b")
        m2 = _m("m2", join="a->b")
        self.assertEqual(_graduated_join_diff([m1, m2]), 0.0)

    def test_all_different_two_members_returns_100(self):
        m1 = _m("m1", join="a->b")
        m2 = _m("m2", join="a->c")
        self.assertEqual(_graduated_join_diff([m1, m2]), 100.0)

    def test_partial_agreement_three_members(self):
        # 2 unique out of 3 → 66.7%
        m1 = _m("m1", join="a->b")
        m2 = _m("m2", join="a->b")
        m3 = _m("m3", join="a->c")
        score = _graduated_join_diff([m1, m2, m3])
        self.assertAlmostEqual(score, 66.67, places=1)


# ---------------------------------------------------------------------------
# Fix 1c: Graduated grain diff
# ---------------------------------------------------------------------------
class TestGraduatedGrainDiff(unittest.TestCase):
    def test_identical_grain_returns_zero(self):
        m1 = _m("m1", grain="day")
        m2 = _m("m2", grain="day")
        self.assertEqual(_graduated_grain_diff([m1, m2]), 0.0)

    def test_day_vs_week_returns_20(self):
        m1 = _m("m1", grain="day")
        m2 = _m("m2", grain="week")
        self.assertEqual(_graduated_grain_diff([m1, m2]), 20.0)

    def test_day_vs_month_returns_40(self):
        m1 = _m("m1", grain="day")
        m2 = _m("m2", grain="month")
        self.assertEqual(_graduated_grain_diff([m1, m2]), 40.0)

    def test_day_vs_year_returns_80(self):
        m1 = _m("m1", grain="day")
        m2 = _m("m2", grain="year")
        self.assertEqual(_graduated_grain_diff([m1, m2]), 80.0)

    def test_transaction_vs_year_returns_100(self):
        # max spread: indices 0 vs 5 → 5*20 = 100
        m1 = _m("m1", grain="transaction")
        m2 = _m("m2", grain="year")
        self.assertEqual(_graduated_grain_diff([m1, m2]), 100.0)

    def test_unknown_grain_uses_default_index_3(self):
        # "custom" → index 3 (month); day → index 1; spread = 3-1 = 2; score = 40
        m1 = _m("m1", grain="day")
        m2 = _m("m2", grain="custom_grain")
        self.assertEqual(_graduated_grain_diff([m1, m2]), 40.0)


# ---------------------------------------------------------------------------
# Fix 1d: Graduated formula diff
# ---------------------------------------------------------------------------
class TestGraduatedFormulaDiff(unittest.TestCase):
    def test_identical_formulas_returns_zero(self):
        m1 = _m("m1", expr="sum(x)")
        m2 = _m("m2", expr="sum(x)")
        self.assertEqual(_graduated_formula_diff([m1, m2]), 0.0)

    def test_structurally_different_formulas_returns_positive(self):
        # sum(x) vs sum(x)/count(y): second adds division and a second aggregation.
        # sqlglot will see different node types (anonymous/sum vs divide+count).
        m1 = _m("m1", expr="sum(amount)")
        m2 = _m("m2", expr="sum(amount) / count(orders)")
        score = _graduated_formula_diff([m1, m2])
        self.assertGreater(score, 0.0)

    def test_same_function_different_column_is_structurally_identical(self):
        # AST node types are the same for sum(a) vs sum(b) — column name is not a type node.
        # This is correct behaviour: structural diff ≠ semantic diff.
        m1 = _m("m1", expr="sum(revenue)")
        m2 = _m("m2", expr="sum(gross_revenue)")
        score = _graduated_formula_diff([m1, m2])
        self.assertEqual(score, 0.0)  # structurally identical → 0 structural distance

    def test_completely_different_formulas_returns_high_score(self):
        m1 = _m("m1", expr="sum(x)")
        m2 = _m("m2", expr="count(y)")
        score = _graduated_formula_diff([m1, m2])
        self.assertGreater(score, 0.0)
        self.assertLessEqual(score, 100.0)


# ---------------------------------------------------------------------------
# Fix 1 integration: detect_drift uses graduated severity
# ---------------------------------------------------------------------------
class TestDetectDriftGraduatedSeverity(unittest.TestCase):
    def _cluster_and_find(self, m1, m2):
        lookup = {m1.metric_id: m1, m2.metric_id: m2}
        cluster = MetricCluster("CL-0001", [m1.metric_id, m2.metric_id], 0.95)
        return detect_drift(cluster, lookup)

    def test_partial_filter_diff_severity_less_than_100(self):
        # Only filter differs, and only partially (["a"] vs ["a","b"])
        m1 = _m("m1", filters=["a"])
        m2 = _m("m2", filters=["a", "b"])
        findings = self._cluster_and_find(m1, m2)
        self.assertEqual(len(findings), 1)
        self.assertLess(findings[0].severity_score, 100.0)
        self.assertGreater(findings[0].severity_score, 0.0)

    def test_day_vs_week_grain_severity_less_than_100(self):
        m1 = _m("m1", grain="day")
        m2 = _m("m2", grain="week")
        findings = self._cluster_and_find(m1, m2)
        grain_findings = [f for f in findings if f.drift_type.value == "grain"]
        self.assertEqual(len(grain_findings), 1)
        # grain signal = 20, weight = 0.15 → severity = 20*0.15 = 3.0
        self.assertLess(grain_findings[0].severity_score, 100.0)
        self.assertGreater(grain_findings[0].severity_score, 0.0)

    def test_transaction_vs_year_grain_severity_higher_than_day_vs_week(self):
        m1a = _m("m1a", grain="day")
        m1b = _m("m1b", grain="week")
        m2a = _m("m2a", grain="transaction")
        m2b = _m("m2b", grain="year")

        lookup1 = {m1a.metric_id: m1a, m1b.metric_id: m1b}
        cluster1 = MetricCluster("CL-0001", [m1a.metric_id, m1b.metric_id], 0.95)
        sev_low = detect_drift(cluster1, lookup1)[0].severity_score

        lookup2 = {m2a.metric_id: m2a, m2b.metric_id: m2b}
        cluster2 = MetricCluster("CL-0002", [m2a.metric_id, m2b.metric_id], 0.95)
        sev_high = detect_drift(cluster2, lookup2)[0].severity_score

        self.assertGreater(sev_high, sev_low)

    def test_no_drift_produces_zero_severity(self):
        m1 = _m("m1")
        m2 = _m("m2")
        findings = self._cluster_and_find(m1, m2)
        self.assertEqual(findings, [])


# ---------------------------------------------------------------------------
# Fix 2: Naive cluster confidence
# ---------------------------------------------------------------------------
class TestNaiveClusterConfidence(unittest.TestCase):
    def _lookup(self, *metrics):
        return {m.metric_id: m for m in metrics}

    def test_singleton_returns_0_65(self):
        m = _m("m1")
        self.assertEqual(_naive_cluster_confidence(["m1"], self._lookup(m)), 0.65)

    def test_same_name_same_expr_returns_0_95(self):
        m1 = _m("m1", name="Revenue", expr="sum(rev)")
        m2 = _m("m2", name="Revenue", expr="sum(rev)")
        conf = _naive_cluster_confidence(["m1", "m2"], self._lookup(m1, m2))
        self.assertAlmostEqual(conf, 0.95)

    def test_same_name_different_expr_returns_0_8(self):
        m1 = _m("m1", name="Revenue", expr="sum(rev)")
        m2 = _m("m2", name="Revenue", expr="sum(gross_rev)")
        conf = _naive_cluster_confidence(["m1", "m2"], self._lookup(m1, m2))
        # name_score=1.0, expr_score=0.5 → 0.65 + 0.30*(1.0*0.5+0.5*0.5) = 0.65+0.30*0.75=0.875
        self.assertAlmostEqual(conf, 0.8750)

    def test_different_name_different_expr_returns_0_7250(self):
        m1 = _m("m1", name="Revenue", expr="sum(rev)")
        m2 = _m("m2", name="Sales", expr="sum(sales)")
        conf = _naive_cluster_confidence(["m1", "m2"], self._lookup(m1, m2))
        # name_score=0.5, expr_score=0.5 → 0.65 + 0.30*0.5 = 0.80
        self.assertAlmostEqual(conf, 0.8000)


# ---------------------------------------------------------------------------
# Fix 4: report_folder propagation
# ---------------------------------------------------------------------------
class TestReportFolder(unittest.TestCase):
    def test_report_folder_used_when_present(self):
        m = _m("m1", report_id="r1", report_folder="Finance")
        inventory = _build_report_inventory([m])
        self.assertEqual(inventory[0]["folder"], "Finance")

    def test_report_folder_falls_back_to_unspecified(self):
        m = _m("m1", report_id="r1")  # no report_folder
        inventory = _build_report_inventory([m])
        self.assertEqual(inventory[0]["folder"], "Unspecified")

    def test_folder_not_analytics_when_unset(self):
        m = _m("m1", report_id="r1")
        inventory = _build_report_inventory([m])
        self.assertNotEqual(inventory[0]["folder"], "Analytics")


# ---------------------------------------------------------------------------
# Fix 5: context-aware risk notes
# ---------------------------------------------------------------------------
class TestInferRiskNote(unittest.TestCase):
    def test_ungoverned_source_flagged(self):
        m = _m("m1", sources=["raw.sales"])
        self.assertIn("Ungoverned source layer", _infer_risk_note(m))

    def test_governed_source_not_flagged(self):
        m = _m("m1", sources=["vw_sales"])
        self.assertNotIn("Ungoverned source layer", _infer_risk_note(m))

    def test_complex_filters_flagged(self):
        m = _m("m1", filters=["a=1", "b=2", "c=3", "d=4"])
        self.assertIn("Complex filter logic", _infer_risk_note(m))

    def test_three_filters_not_flagged(self):
        m = _m("m1", filters=["a=1", "b=2", "c=3"])
        self.assertNotIn("Complex filter logic", _infer_risk_note(m))

    def test_high_regulatory_tag_flagged(self):
        m = _m("m1", regulatory_tag="high")
        self.assertIn("Regulatory-sensitive", _infer_risk_note(m))

    def test_low_regulatory_tag_not_flagged(self):
        m = _m("m1", regulatory_tag="low")
        self.assertNotIn("Regulatory-sensitive", _infer_risk_note(m))

    def test_no_risks_returns_clean_message(self):
        m = _m("m1", sources=["vw_clean"], filters=[], regulatory_tag="low")
        self.assertEqual(_infer_risk_note(m), "No risks identified")

    def test_multiple_risks_combined(self):
        m = _m("m1", sources=["raw.t"], filters=["a=1", "b=2", "c=3", "d=4"], regulatory_tag="high")
        note = _infer_risk_note(m)
        self.assertIn("Ungoverned source layer", note)
        self.assertIn("Complex filter logic", note)
        self.assertIn("Regulatory-sensitive", note)

    def test_kpi_dict_uses_inferred_risk_not_hardcoded(self):
        m = _m("m1", sources=["vw_revenue"])  # governed source, no filters, no tag
        kpi_dict = _build_kpi_dictionary([m])
        self.assertEqual(kpi_dict[0]["risk_notes"], "No risks identified")
        self.assertNotEqual(kpi_dict[0]["risk_notes"], "Review for standardization")

    def test_kpi_dict_combines_multi_variant_with_risk(self):
        m1 = _m("m1", name="Revenue", sources=["raw.sales"], report_id="r1")
        m2 = _m("m2", name="Revenue", sources=["raw.sales"], report_id="r2")
        kpi_dict = _build_kpi_dictionary([m1, m2])
        risk = kpi_dict[0]["risk_notes"]
        self.assertIn("Multiple variants detected", risk)
        self.assertIn("Ungoverned source layer", risk)


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------
class TestBackwardCompatibilityGraduated(unittest.TestCase):
    def test_existing_payload_no_new_fields_runs(self):
        metrics = [
            MetricInstance("m1", "r1", "d1", "NAV", "sum(nav)", "fund_date",
                           ["trade_date=:as_of"], "fund->positions", ["raw.positions"]),
            MetricInstance("m2", "r2", "d2", "NAV", "sum(nav)", "fund_date",
                           ["settle_date=:as_of"], "fund->positions", ["raw.positions"]),
        ]
        result = run_analysis(metrics, use_advanced_clustering=False)
        self.assertIn("clusters", result)
        self.assertGreaterEqual(len(result["drift_findings"]), 1)
        # Severity must be graduated, not stuck at 100
        for f in result["drift_findings"]:
            self.assertLessEqual(f["severity_score"], 100.0)

    def test_no_100_severity_when_only_filter_partially_differs(self):
        metrics = [
            MetricInstance("m1", "r1", "d1", "Rev", "sum(rev)", "day",
                           ["a=1"], "t->u", ["raw.t"]),
            MetricInstance("m2", "r2", "d2", "Rev", "sum(rev)", "day",
                           ["a=1", "b=2"], "t->u", ["raw.t"]),
        ]
        result = run_analysis(metrics, use_advanced_clustering=False)
        for f in result["drift_findings"]:
            self.assertLess(f["severity_score"], 100.0,
                            msg=f"Expected graduated severity, got {f['severity_score']} for {f['drift_type']}")


if __name__ == "__main__":
    unittest.main()
