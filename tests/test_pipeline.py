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


if __name__ == "__main__":
    unittest.main()
