import json

from src.accelerator.models import MetricInstance
from src.accelerator.pipeline import run_analysis

def main():
    print("Running project sample analysis...")
    metrics = [
        MetricInstance(
            metric_id="m1",
            report_id="r1",
            dataset_id="d1",
            metric_name="Revenue",
            expression_signature="sum(revenue)",
            grain="day",
            filters=["region = 'US'"],
            join_path_signature="sales->region",
            source_objects=["raw.sales"]
        ),
        MetricInstance(
            metric_id="m2",
            report_id="r2",
            dataset_id="d2",
            metric_name="Revenue",
            expression_signature="sum(revenue)",
            grain="day",
            filters=["region = 'EMEA'", "status = 'closed'"], # filter diff
            join_path_signature="sales->region",
            source_objects=["raw.sales"]
        ),
        MetricInstance(
            metric_id="m3",
            report_id="r3",
            dataset_id="d3",
            metric_name="Revenue",
            expression_signature="sum(revenue)",
            grain="month", # grain diff and filter diff
            filters=["region = 'APAC'"],
            join_path_signature="sales->region",
            source_objects=["raw.sales"]
        )
    ]
    
    result = run_analysis(metrics)
    print(json.dumps(result, indent=2))
    print("\nAnalysis complete.")

if __name__ == "__main__":
    main()
