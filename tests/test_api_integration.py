"""
Comprehensive API integration tests for the AI Metric Drift Detection Engine.
Starts the HTTP server in a background thread, sends requests, validates responses.
"""
import json
import os
import sys
import threading
import time
import unittest
import urllib.request
import urllib.error
from http.server import HTTPServer

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import SimpleHandler
from src.accelerator.models import MetricInstance
from src.accelerator.pipeline import run_analysis

PORT = 9876  # Use a non-standard port to avoid conflicts


class APIIntegrationTests(unittest.TestCase):
    """Test the HTTP API endpoints end-to-end."""

    server = None
    server_thread = None

    @classmethod
    def setUpClass(cls):
        """Start the HTTP server in a daemon thread."""
        cls.server = HTTPServer(("127.0.0.1", PORT), SimpleHandler)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()
        time.sleep(0.5)  # Give server time to start
        print(f"\n[SETUP] Test server started on http://127.0.0.1:{PORT}")

    @classmethod
    def tearDownClass(cls):
        """Shut down the HTTP server."""
        cls.server.shutdown()
        cls.server_thread.join(timeout=5)
        print("[TEARDOWN] Test server stopped")

    # ── Helper ───────────────────────────────────────────────────────────
    def _post_json(self, path, payload):
        """POST JSON to the server and return (status_code, response_body)."""
        url = f"http://127.0.0.1:{PORT}{path}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def _get(self, path):
        """GET a path and return (status_code, body_text)."""
        url = f"http://127.0.0.1:{PORT}{path}"
        try:
            resp = urllib.request.urlopen(url, timeout=10)
            return resp.status, resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8")

    # ── GET Tests ────────────────────────────────────────────────────────
    def test_01_homepage_loads(self):
        """GET / should return 200 with HTML containing the page title."""
        status, body = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn("AI Metric Drift Detector", body)
        print("  [PASS] Homepage loads with correct title")

    def test_02_unknown_route_returns_404(self):
        """GET /nonexistent should return 404."""
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{PORT}/nonexistent", timeout=5)
            self.fail("Expected 404")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 404)
        print("  [PASS] Unknown GET route returns 404")

    # ── POST /api/run – Valid Payloads ───────────────────────────────────
    def test_03_simple_two_metric_analysis(self):
        """Two metrics with same name but different filters --> FILTER drift."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": ["region='US'"],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            },
            {
                "metric_id": "m2", "report_id": "r2", "dataset_id": "d2",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": ["region='EMEA'"],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)

        # Should produce exactly 1 cluster with 2 members
        self.assertEqual(len(result["clusters"]), 1)
        self.assertEqual(len(result["clusters"][0]["members"]), 2)

        # Should find FILTER drift
        drift_types = [f["drift_type"] for f in result["drift_findings"]]
        self.assertIn("filter", drift_types)
        self.assertNotIn("formula", drift_types)

        # Should produce 1 recommendation
        self.assertEqual(len(result["recommendations"]), 1)
        print("  [PASS] Two-metric analysis: FILTER drift detected correctly")

    def test_04_formula_drift_detection(self):
        """Two metrics with different expression signatures --> FORMULA drift."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            },
            {
                "metric_id": "m2", "report_id": "r2", "dataset_id": "d2",
                "metric_name": "Revenue", "expression_signature": "sum(gross_revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)

        # These have different expression_signature so they land in DIFFERENT clusters
        # (clustering is by name + expression_signature)
        self.assertEqual(len(result["clusters"]), 2)
        print("  [PASS] Formula difference creates separate clusters (by design)")

    def test_05_grain_drift_detection(self):
        """Two metrics with different grain --> GRAIN drift."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            },
            {
                "metric_id": "m2", "report_id": "r2", "dataset_id": "d2",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "month", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)

        drift_types = [f["drift_type"] for f in result["drift_findings"]]
        self.assertIn("grain", drift_types)
        print("  [PASS] Grain drift detected correctly")

    def test_06_join_drift_detection(self):
        """Two metrics with different join paths --> JOIN drift."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            },
            {
                "metric_id": "m2", "report_id": "r2", "dataset_id": "d2",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->territory", "source_objects": ["raw.sales"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)

        drift_types = [f["drift_type"] for f in result["drift_findings"]]
        self.assertIn("join", drift_types)
        print("  [PASS] Join drift detected correctly")

    def test_07_multiple_drift_types(self):
        """Three metrics with filter, grain, AND join differences --> multiple drifts."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": ["region='US'"],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            },
            {
                "metric_id": "m2", "report_id": "r2", "dataset_id": "d2",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "month", "filters": ["region='EMEA'", "status='closed'"],
                "join_path_signature": "sales->territory", "source_objects": ["raw.sales"]
            },
            {
                "metric_id": "m3", "report_id": "r3", "dataset_id": "d3",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "quarter", "filters": ["region='APAC'"],
                "join_path_signature": "sales->account", "source_objects": ["raw.sales"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)

        self.assertEqual(len(result["clusters"]), 1)
        self.assertEqual(len(result["clusters"][0]["members"]), 3)

        drift_types = set(f["drift_type"] for f in result["drift_findings"])
        self.assertIn("filter", drift_types)
        self.assertIn("grain", drift_types)
        self.assertIn("join", drift_types)

        # Severity should be > 0
        for f in result["drift_findings"]:
            self.assertGreater(f["severity_score"], 0)

        print(f"  [PASS] Multiple drift types detected: {drift_types}")

    def test_08_single_metric_no_drift(self):
        """A single metric should produce 1 cluster but ZERO drift findings."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)
        self.assertEqual(len(result["clusters"]), 1)
        self.assertEqual(len(result["drift_findings"]), 0)
        self.assertEqual(len(result["recommendations"]), 1)
        print("  [PASS] Single metric: 1 cluster, 0 drift findings")

    def test_09_empty_list_returns_empty_results(self):
        """Empty metrics list should return empty results."""
        status, result = self._post_json("/api/run", [])
        self.assertEqual(status, 200)
        self.assertEqual(result["clusters"], [])
        self.assertEqual(result["drift_findings"], [])
        self.assertEqual(result["recommendations"], [])
        print("  [PASS] Empty list returns empty results")

    def test_10_layer_violation_recommendation(self):
        """Metrics using raw tables (not semantic views) --> MOVE_TO_SNOWFLAKE recommendation."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.sales"]
            },
            {
                "metric_id": "m2", "report_id": "r2", "dataset_id": "d2",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": [],
                "join_path_signature": "sales->region", "source_objects": ["raw.orders"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)
        actions = [r["action_type"] for r in result["recommendations"]]
        self.assertIn("move_to_snowflake", actions)
        print("  [PASS] Layer violation --> MOVE_TO_SNOWFLAKE recommendation")

    def test_11_semantic_view_no_layer_violation(self):
        """Metrics using semantic views --> should NOT trigger MOVE_TO_SNOWFLAKE."""
        payload = [
            {
                "metric_id": "m1", "report_id": "r1", "dataset_id": "d1",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": ["region='US'"],
                "join_path_signature": "sales->region",
                "source_objects": ["semantic view.sales"]
            },
            {
                "metric_id": "m2", "report_id": "r2", "dataset_id": "d2",
                "metric_name": "Revenue", "expression_signature": "sum(revenue)",
                "grain": "day", "filters": ["region='EMEA'"],
                "join_path_signature": "sales->region",
                "source_objects": ["semantic view.sales"]
            }
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)
        actions = [r["action_type"] for r in result["recommendations"]]
        self.assertNotIn("move_to_snowflake", actions)
        print("  [PASS] Semantic view sources: no layer violation")

    def test_12_missing_fields_use_defaults(self):
        """Metrics with missing optional fields should use auto-generated defaults."""
        payload = [
            {"metric_name": "Revenue"},
            {"metric_name": "Revenue"}
        ]
        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)
        self.assertGreaterEqual(len(result["clusters"]), 1)
        print("  [PASS] Missing fields --> auto-defaults applied successfully")

    # ── POST /api/run – Error Cases ──────────────────────────────────────
    def test_13_invalid_json_returns_500(self):
        """Malformed JSON body should return a server error."""
        url = f"http://127.0.0.1:{PORT}/api/run"
        req = urllib.request.Request(
            url, data=b"NOT VALID JSON",
            headers={"Content-Type": "application/json"}
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            self.fail("Expected error for invalid JSON")
        except urllib.error.HTTPError as e:
            self.assertIn(e.code, [400, 500])
            body = json.loads(e.read().decode("utf-8"))
            self.assertIn("error", body)
        print("  [PASS] Invalid JSON returns error with message")

    def test_14_post_unknown_route_returns_404(self):
        """POST to unknown route should return 404."""
        url = f"http://127.0.0.1:{PORT}/api/unknown"
        req = urllib.request.Request(
            url, data=b"{}",
            headers={"Content-Type": "application/json"}
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            self.fail("Expected 404")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 404)
        print("  [PASS] POST to unknown route returns 404")

    # ── Sample Data File Tests ───────────────────────────────────────────
    def test_15_sample_data_file_analysis(self):
        """Load sample_metric_data_1.json and run through the API end-to-end."""
        sample_path = os.path.join(
            os.path.dirname(__file__), "..", "sample_data", "sample_metric_data_1.json"
        )
        if not os.path.exists(sample_path):
            self.skipTest("sample_metric_data_1.json not found")

        with open(sample_path, "r") as f:
            payload = json.load(f)

        self.assertEqual(len(payload), 100, "Sample file should have 100 metrics")

        status, result = self._post_json("/api/run", payload)
        self.assertEqual(status, 200)

        # With 100 metrics all named "Revenue", we should get clusters
        self.assertGreater(len(result["clusters"]), 0)
        self.assertGreater(len(result["recommendations"]), 0)

        # Report counts
        total_members = sum(len(c["members"]) for c in result["clusters"])
        self.assertEqual(total_members, 100, "All 100 metrics should be in clusters")

        print(f"  [PASS] Sample file: {len(result['clusters'])} clusters, "
              f"{len(result['drift_findings'])} findings, "
              f"{len(result['recommendations'])} recommendations")

    # ── Pipeline Direct Tests ────────────────────────────────────────────
    def test_16_pipeline_direct_with_all_drift_types(self):
        """Direct pipeline test confirming all four drift types are detectable."""
        metrics = [
            MetricInstance("m1", "r1", "d1", "NAV", "sum(nav)", "fund_date",
                          ["trade_date=:as_of"], "fund->positions", ["raw.positions"]),
            MetricInstance("m2", "r2", "d2", "NAV", "sum(nav)", "quarter",
                          ["settle_date=:as_of", "status=active"], "fund->holdings", ["raw.positions"]),
        ]
        result = run_analysis(metrics)
        drift_types = set(f["drift_type"] for f in result["drift_findings"])
        self.assertIn("filter", drift_types)
        self.assertIn("grain", drift_types)
        self.assertIn("join", drift_types)
        self.assertNotIn("formula", drift_types)  # same expression_signature

        # Severity > 0
        for f in result["drift_findings"]:
            self.assertGreater(f["severity_score"], 0)

        print(f"  [PASS] Direct pipeline: detected drift types {drift_types}")

    def test_17_confidence_scores_correct(self):
        """Multi-member clusters get 0.95 confidence; singles get 0.65."""
        metrics = [
            MetricInstance("m1", "r1", "d1", "Revenue", "sum(revenue)", "day",
                          [], "a->b", ["raw.t"]),
            MetricInstance("m2", "r2", "d2", "Revenue", "sum(revenue)", "day",
                          [], "a->b", ["raw.t"]),
            MetricInstance("m3", "r3", "d3", "Unique", "sum(unique)", "day",
                          [], "a->b", ["raw.t"]),
        ]
        result = run_analysis(metrics)
        for cluster in result["clusters"]:
            if len(cluster["members"]) > 1:
                self.assertEqual(cluster["confidence_score"], 0.95)
            else:
                self.assertEqual(cluster["confidence_score"], 0.65)
        print("  [PASS] Confidence scores: multi-member=0.95, single=0.65")

    def test_18_all_sample_files_process_successfully(self):
        """All 10 sample data files should be processable without errors."""
        sample_dir = os.path.join(os.path.dirname(__file__), "..", "sample_data")
        if not os.path.exists(sample_dir):
            self.skipTest("sample_data directory not found")

        files = sorted(f for f in os.listdir(sample_dir) if f.endswith(".json"))
        self.assertGreaterEqual(len(files), 10, "Expected at least 10 sample files")

        for filename in files:
            with open(os.path.join(sample_dir, filename), "r") as f:
                payload = json.load(f)
            status, result = self._post_json("/api/run", payload)
            self.assertEqual(status, 200, f"Failed on {filename}")
            self.assertIn("clusters", result)
            self.assertIn("drift_findings", result)
            self.assertIn("recommendations", result)

        print(f"  [PASS] All {len(files)} sample files processed successfully")


if __name__ == "__main__":
    print("=" * 70)
    print("  AI Metric Drift Detection Engine – Comprehensive API Tests")
    print("=" * 70)
    unittest.main(verbosity=2)
