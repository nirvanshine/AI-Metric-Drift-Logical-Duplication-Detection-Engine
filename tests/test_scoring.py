import unittest

from src.accelerator.scoring import (
    ConsolidationSignals,
    DriftSignals,
    consolidation_priority_score,
    drift_severity_score,
)


class ScoringTests(unittest.TestCase):
    def test_drift_severity_weighting(self):
        score = drift_severity_score(DriftSignals(100, 100, 100, 100))
        self.assertEqual(score, 100.0)

    def test_drift_severity_partial(self):
        score = drift_severity_score(DriftSignals(100, 0, 0, 0))
        self.assertEqual(score, 40.0)

    def test_consolidation_priority_weighting(self):
        score = consolidation_priority_score(ConsolidationSignals(80, 50, 90, 70, 60))
        self.assertEqual(score, 71.5)


if __name__ == "__main__":
    unittest.main()
