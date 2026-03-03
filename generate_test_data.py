import os
import json
import random
import uuid

drifts = ['formula', 'filter', 'join', 'grain', 'none', 'none'] # skewed towards duplicates ('none')
reports = ['Sales Dashboard', 'Exec Summary', 'Marketing ROI', 'Daily Ops', 'Weekly Metrics']
datasets = ['Core_Sales', 'Agg_Users', 'Marketing_Events', 'Finance_Ledger']
base_metrics = [
    {"name": "Revenue", "sig": "sum(revenue)", "filters": ["region='US'", "status='closed'"], "joins": ["sales->region", "sales->account"]},
    {"name": "Active Users", "sig": "count_distinct(user_id)", "filters": ["is_active=TRUE", "plan='Premium'"], "joins": ["users->sessions", "users->subscriptions"]},
    {"name": "Churn Rate", "sig": "sum(churned)/count(users)", "filters": ["date >= '2023-01-01'", "segment='Enterprise'"], "joins": ["users->cancellations", "users->accounts"]}
]

os.makedirs("sample_data", exist_ok=True)

for i in range(1, 11):
    filename = f"sample_metric_data_{i}.json"
    metrics = []
    
    num_records = random.randint(150, 250)
    for j in range(num_records):
        metric_type = random.choice(drifts)
        base = random.choice(base_metrics)
        
        signature = base["sig"]
        filter_list = []
        join_path = base["joins"][0]
        grain = "day"
        
        if metric_type == 'formula':
            signature = signature + " + 0" # slight formula tweak
        elif metric_type == 'filter':
            filter_list = [random.choice(base["filters"])]
        elif metric_type == 'join':
            join_path = random.choice(base["joins"])
        elif metric_type == 'grain':
            grain = random.choice(["day", "month", "week"])
            
        obj = {
            "metric_id": str(uuid.uuid4())[:8],
            "report_id": random.choice(reports),
            "dataset_id": random.choice(datasets),
            "metric_name": base["name"],
            "expression_signature": signature,
            "grain": grain,
            "filters": filter_list,
            "join_path_signature": join_path,
            "source_objects": ["raw.events"] if random.random() > 0.05 else ["semantic.core"] 
        }
        metrics.append(obj)
        
    with open(os.path.join("sample_data", filename), "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Created {filename} with {num_records} metrics")
