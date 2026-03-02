import unittest

from src.accelerator.demo_ingest import extract_metrics_from_sql, parse_report_upload


class DemoIngestTests(unittest.TestCase):
    def test_extract_metrics_from_sql(self):
        sql = "SELECT SUM(nav) AS NAV, COUNT(*) AS ROWS FROM finance.positions WHERE trade_date = :as_of AND region = 'US'"
        metrics = extract_metrics_from_sql("r1", "d1", sql)
        self.assertEqual(len(metrics), 2)
        self.assertEqual(metrics[0].metric_name.lower(), "nav")
        self.assertIn("trade_date = :as_of", metrics[0].filters)

    def test_parse_report_upload(self):
        payload = """
        {
          "reports": [
            {
              "report_id": "rpt1",
              "datasets": [
                {
                  "dataset_id": "ds1",
                  "sql_text": "SELECT SUM(nav) AS NAV FROM finance.positions"
                }
              ]
            }
          ]
        }
        """
        metrics = parse_report_upload(payload)
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0].report_id, "rpt1")


if __name__ == "__main__":
    unittest.main()
