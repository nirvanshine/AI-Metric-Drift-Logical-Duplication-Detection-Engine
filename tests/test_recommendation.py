import unittest

from src.accelerator.models import ActionType
from src.accelerator.recommendation import RecommendationInputs, choose_action, priority_score, rationale


class RecommendationTests(unittest.TestCase):
    def test_choose_action_move_to_snowflake(self):
        inputs = RecommendationInputs(
            drift_severity=90, duplication_count=1, regulatory_sensitivity=80,
            usage_frequency=50, complexity=50, layer_violation=True
        )
        self.assertEqual(choose_action(inputs), ActionType.MOVE_TO_SNOWFLAKE)

    def test_choose_action_rebuild(self):
        inputs = RecommendationInputs(
            drift_severity=85, duplication_count=2, regulatory_sensitivity=75,
            usage_frequency=50, complexity=50, layer_violation=False
        )
        self.assertEqual(choose_action(inputs), ActionType.REBUILD)

    def test_choose_action_consolidate(self):
        inputs = RecommendationInputs(
            drift_severity=40, duplication_count=3, regulatory_sensitivity=60,
            usage_frequency=50, complexity=50, layer_violation=False
        )
        self.assertEqual(choose_action(inputs), ActionType.CONSOLIDATE)

    def test_choose_action_retire(self):
        inputs = RecommendationInputs(
            drift_severity=60, duplication_count=2, regulatory_sensitivity=60,
            usage_frequency=20, complexity=50, layer_violation=False
        )
        self.assertEqual(choose_action(inputs), ActionType.RETIRE)

    def test_choose_action_standardize(self):
        inputs = RecommendationInputs(
            drift_severity=60, duplication_count=1, regulatory_sensitivity=60,
            usage_frequency=50, complexity=50, layer_violation=False
        )
        self.assertEqual(choose_action(inputs), ActionType.STANDARDIZE)

    def test_priority_score_calculation(self):
        inputs = RecommendationInputs(
            drift_severity=100, duplication_count=5, regulatory_sensitivity=100,
            usage_frequency=100, complexity=100, layer_violation=False
        )
        # drift (30) + dup (20) + reg (20) + usage (15) + comp (15) = 100
        self.assertEqual(priority_score(inputs), 100.0)

    def test_rationale_generation(self):
        inputs = RecommendationInputs(
            drift_severity=40, duplication_count=3, regulatory_sensitivity=60,
            usage_frequency=50, complexity=50
        )
        action = choose_action(inputs)
        r = rationale(inputs, action)
        self.assertIn("merge opportunity", r)


if __name__ == "__main__":
    unittest.main()
