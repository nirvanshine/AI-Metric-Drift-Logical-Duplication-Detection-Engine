"""Tests verifying the 7-section output pack and Excel generation."""
import os
import tempfile
import unittest

from src.accelerator.models import MetricInstance
from src.accelerator.pipeline import run_analysis


def _make_metrics():
    return [
        MetricInstance(
            metric_id="m1",
            report_id="r1",
            dataset_id="d1",
            metric_name="NAV",
            expression_signature="sum(nav)",
            grain="fund_date",
            filters=["trade_date = :as_of"],
            join_path_signature="fund->positions",
            source_objects=["raw.positions"],
        ),
        MetricInstance(
            metric_id="m2",
            report_id="r2",
            dataset_id="d2",
            metric_name="NAV",
            expression_signature="sum(nav)",
            grain="fund_date",
            filters=["settle_date = :as_of"],
            join_path_signature="fund->positions",
            source_objects=["raw.positions"],
        ),
        MetricInstance(
            metric_id="m3",
            report_id="r1",
            dataset_id="d1",
            metric_name="Revenue",
            expression_signature="sum(rev)",
            grain="day",
            filters=[],
            join_path_signature="orders->items",
            source_objects=["raw.orders"],
        ),
    ]


class TestSevenSectionOutput(unittest.TestCase):
    def setUp(self):
        self.metrics = _make_metrics()
        self.result = run_analysis(self.metrics, use_advanced_clustering=False)

    def test_all_seven_keys_present(self):
        expected_keys = {
            "executive_scorecard",
            "report_inventory",
            "kpi_dictionary",
            "clusters",
            "drift_findings",
            "recommendations",
            "how_to_read",
        }
        self.assertEqual(set(self.result.keys()), expected_keys)

    def test_executive_scorecard_counts(self):
        sc = self.result["executive_scorecard"]
        # 2 unique reports (r1, r2)
        self.assertEqual(sc["reports_scanned"], 2)
        # 2 unique KPI names (nav, revenue)
        self.assertEqual(sc["kpis_inferred"], 2)
        # clusters_formed must match actual cluster count
        self.assertEqual(sc["clusters_formed"], len(self.result["clusters"]))
        # drift_findings_count must match actual findings count
        self.assertEqual(sc["drift_findings_count"], len(self.result["drift_findings"]))
        # highest_severity must be >= 0
        self.assertGreaterEqual(sc["highest_severity"], 0.0)
        # run_id and generated_at must be present
        self.assertTrue(sc.get("run_id"))
        self.assertTrue(sc.get("generated_at"))

    def test_report_inventory_entries(self):
        inventory = self.result["report_inventory"]
        report_ids = {r["report_id"] for r in inventory}
        self.assertEqual(report_ids, {"r1", "r2"})
        for entry in inventory:
            self.assertIn("name", entry)
            self.assertIn("folder", entry)
            self.assertIn("dataset_count", entry)

    def test_kpi_dictionary_unique_names(self):
        kpi_dict = self.result["kpi_dictionary"]
        kpi_names_lower = {k["kpi_name"].lower() for k in kpi_dict}
        unique_metric_names = {m.metric_name.lower() for m in self.metrics}
        self.assertEqual(kpi_names_lower, unique_metric_names)

    def test_kpi_dictionary_fields(self):
        for entry in self.result["kpi_dictionary"]:
            self.assertIn("inferred_definition", entry)
            self.assertIn("grain", entry)
            self.assertIn("primary_source", entry)
            self.assertIn("computed_in_layer", entry)
            self.assertIn("found_in_reports", entry)
            self.assertIn("risk_notes", entry)
            self.assertIsInstance(entry["found_in_reports"], list)

    def test_clusters_enriched(self):
        for cluster in self.result["clusters"]:
            self.assertIn("metric_intent", cluster)
            self.assertIn("reports", cluster)
            self.assertIn("duplicate_count", cluster)
            self.assertIsInstance(cluster["reports"], list)

    def test_drift_findings_enriched(self):
        for finding in self.result["drift_findings"]:
            self.assertIn("kpi_name", finding)
            self.assertIn("impact", finding)
            self.assertIn("recommendation_text", finding)

    def test_recommendations_enriched(self):
        for rec in self.result["recommendations"]:
            self.assertIn("area", rec)
            self.assertIn("priority_label", rec)
            self.assertIn("impacted_reports", rec)
            self.assertIsInstance(rec["impacted_reports"], list)

    def test_priority_labels_valid(self):
        valid_labels = {"P1 (0-30 days)", "P2 (30-90 days)", "P3 (90+ days)"}
        for rec in self.result["recommendations"]:
            self.assertIn(rec["priority_label"], valid_labels)

    def test_p1_count_matches_recommendations(self):
        sc = self.result["executive_scorecard"]
        p1_actual = sum(
            1 for r in self.result["recommendations"]
            if r.get("priority_label", "").startswith("P1")
        )
        self.assertEqual(sc["p1_recommendations"], p1_actual)

    def test_how_to_read_has_seven_entries(self):
        how_to_read = self.result["how_to_read"]
        self.assertEqual(len(how_to_read), 7)
        for entry in how_to_read:
            self.assertIn("section", entry)
            self.assertIn("content", entry)


class TestExcelGeneration(unittest.TestCase):
    def setUp(self):
        self.result = run_analysis(_make_metrics(), use_advanced_clustering=False)

    def test_excel_generates_valid_xlsx(self):
        try:
            import openpyxl  # noqa: F401
        except ImportError:
            self.skipTest("openpyxl not installed")

        from src.accelerator.export import generate_excel_pack

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            path = tmp.name
        try:
            generate_excel_pack(self.result, path)
            self.assertTrue(os.path.exists(path))
            self.assertGreater(os.path.getsize(path), 0)

            import openpyxl
            wb = openpyxl.load_workbook(path)
            expected_sheets = {
                "Executive_Scorecard",
                "Report_Inventory",
                "KPI_Dictionary",
                "Metric_Clusters",
                "Drift_Findings",
                "Recommendations_Backlog",
                "How_to_Read",
            }
            self.assertEqual(set(wb.sheetnames), expected_sheets)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def test_excel_raises_without_openpyxl(self):
        import unittest.mock as mock
        from src.accelerator import export as export_mod

        original = export_mod._OPENPYXL_AVAILABLE
        export_mod._OPENPYXL_AVAILABLE = False
        try:
            with self.assertRaises(RuntimeError):
                export_mod.generate_excel_pack(self.result, "dummy.xlsx")
        finally:
            export_mod._OPENPYXL_AVAILABLE = original


if __name__ == "__main__":
    unittest.main()
