import unittest

from src.accelerator.models import ActionType, DriftType, MetricInstance


class ModelsTests(unittest.TestCase):
    def test_drift_type_enum(self):
        self.assertEqual(DriftType.FORMULA.value, "formula")
        self.assertEqual(DriftType.FILTER.value, "filter")

    def test_action_type_enum(self):
        self.assertEqual(ActionType.STANDARDIZE.value, "standardize")
        self.assertEqual(ActionType.MOVE_TO_SNOWFLAKE.value, "move_to_snowflake")

    def test_metric_instance_creation(self):
        m = MetricInstance(
            metric_id="m1",
            report_id="r1",
            dataset_id="d1",
            metric_name="TEST",
            expression_signature="sum(val)",
            grain="day",
            filters=["active = 1"],
            join_path_signature="a->b",
            source_objects=["table_a"]
        )
        self.assertEqual(m.metric_name, "TEST")
        self.assertEqual(m.parameters_impacting_metric, [])


if __name__ == "__main__":
    unittest.main()
