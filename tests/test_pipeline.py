import unittest

from src.accelerator.models import MetricInstance
from src.accelerator.pipeline import run_analysis


class PipelineTests(unittest.TestCase):
    def test_run_analysis_generates_findings_and_recommendations(self):
        metrics = [
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
        ]

        result = run_analysis(metrics)

        self.assertEqual(len(result["clusters"]), 1)
        self.assertGreaterEqual(len(result["drift_findings"]), 1)
        self.assertEqual(len(result["recommendations"]), 1)

    def test_cluster_metrics_grouping(self):
        from src.accelerator.pipeline import cluster_metrics
        metrics = [
            MetricInstance("m1", "r1", "d1", "Sales", "sum(sales)", "day", [], "a->b", ["table_a"]),
            MetricInstance("m2", "r2", "d2", "sales", "sum(sales)", "day", [], "a->b", ["table_a"]),
            MetricInstance("m3", "r3", "d3", "Revenue", "sum(rev)", "day", [], "a->b", ["table_a"]),
        ]
        clusters = cluster_metrics(metrics)
        self.assertEqual(len(clusters), 2)
        sales_cluster = next(c for c in clusters if "m1" in c.members)
        self.assertIn("m2", sales_cluster.members)
        self.assertEqual(sales_cluster.confidence_score, 0.95)

    def test_detect_drift_variations(self):
        from src.accelerator.models import MetricCluster, DriftType
        from src.accelerator.pipeline import detect_drift
        metrics = [
            MetricInstance("m1", "r1", "d1", "Sales", "sum(sales)", "day", ["a=1"], "a->b", ["table_a"]),
            MetricInstance("m2", "r2", "d2", "Sales", "sum(sales)", "month", ["a=1"], "a->c->b", ["table_a"]), # grain diff, join diff
        ]
        lookup = {m.metric_id: m for m in metrics}
        cluster = MetricCluster("CL-0001", ["m1", "m2"], 0.95)
        findings = detect_drift(cluster, lookup)
        
        drift_types = [f.drift_type for f in findings]
        self.assertIn(DriftType.GRAIN, drift_types)
        self.assertIn(DriftType.JOIN, drift_types)
        self.assertNotIn(DriftType.FORMULA, drift_types)
        self.assertNotIn(DriftType.FILTER, drift_types)


if __name__ == "__main__":
    unittest.main()
