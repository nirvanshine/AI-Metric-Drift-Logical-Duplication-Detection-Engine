# AI-Metric-Drift-Logical-Duplication-Detection-Engine

Python accelerator scaffold for detecting SSRS KPI duplication, metric drift, and BI layer violations, then producing rationalization recommendations.

The primary objective of this project is to detect issues in business metrics and recommend actions to fix them. Specifically, it is built to:

Find Logical Duplication: Identify metrics that are functionally performing the same exact calculation (e.g. tracking "Sales Revenue" multiple times across different configurations).

Detect Metric Drift: Detect "drift", meaning inconsistencies in how identical metrics are being put to use. It detects if different metrics:
  • Use different core formulas (Formula Drift).
  • Apply different sets of filters, such as different data restrictions (Filter Drift).
  • Draw from different source systems or use inconsistent database joins (Join Drift).
  • Are summarized by different data granularities, such as monthly vs. daily (Grain Drift).

Recommend Business Actions: Analyze metrics that shouldn't be separated and logically map out solutions to "rationalize" (clean up) them, offering choices like standardizing the metrics across BI tools, or fully retiring duplications.



## What's Included

- Core domain entities for reports, metrics, clusters, drift findings, and recommendations.
- Weighted scoring models aligned to the PRD/TRD.
- Deterministic pipeline for:
  - clustering metrics
  - detecting drift types (formula/filter/join/grain)
  - generating recommendation actions
- Unit tests for scoring and pipeline behavior.

## How it is Executed
The system is implemented as an "Accelerator Scaffold". Rather than using unreliable natural language modeling natively, the process leverages a deterministic, rule-based approach across 4 main modules. Here is the pipeline flow (src/accelerator/pipeline.py):

Ingestion & Metric Formatting (models.py): Metrics and queries are gathered and transformed into a standardized canonical model, storing the dataset, SQL filters, underlying grain, object sources, and the core expression signature.

Clustering (pipeline.py): The cluster_metrics phase groups the metrics precisely by name and signature. This groups naturally identical implementations together into clusters so they can be analyzed.

Drift Detection & Analysis (pipeline.py & scoring.py): Within a single cluster (a group of presumably similar metrics), the system runs an automated check. It compares the metrics within a cluster and detects any divergence (Drift). It then passes these inputs into a scoring model which calculates an overall score.

Recommendation Resolution (recommendation.py): With the clustering data and severe drift calculations populated, the framework feeds this into an Action generation step. 

Based on logic:
If the business logic is handled fully in an ungoverned BI layer ("Layer Violation") -> Action: MOVE_TO_SNOWFLAKE (Centralize in a semantic view).

High inconsistency and high sensitivity -> Action: REBUILD

No inconsistency, but high repetition frequency -> Action: CONSOLIDATE

No inconsistency, but low usage -> Action: RETIRE

Default fix -> Action: STANDARDIZE

The current execution flow runs via the 

run_analysis()
 function, where teams can input collections of metrics locally and receive detailed output on the clusters, detected discrepancies, and actionable recommendations.

## How it is consumed

The project is built with a modular architecture so it can be consumed in multiple ways:

1. A Web Application (Primary UI)
By running python app.py, the project starts a lightweight local web server. If you navigate to http://localhost:8000 in your browser, you get a full frontend web interface (

index.html
). This allows non-technical users to upload JSON files via a file picker and view a visual, human-readable summary of the analysis in their browser.

2. A REST API
Behind the scenes of the web application, 

app.py
 also exposes a dedicated REST API endpoint at POST /api/analyze. You can use tools like curl, Postman, or external services to send POST requests containing the raw JSON arrays directly to this endpoint and receive the calculated clusters, drift findings, and recommendations back as a JSON response.

3. A Python Module
The actual "brain" of the engine lives inside the src/accelerator/ directory (pipeline.py, models.py, scoring.py, recommendations.py). This is a pure, decoupled Python package. You can import src.accelerator.pipeline into any other broader Python data engineering pipeline (like an Airflow DAG or a custom script) to process metrics programmatically without ever starting a web server.

4. A CLI Script (for Demos)
As referenced in your README changes, there is also a demo_run.py script. While it isn't a complex CLI tool with built-in argument parsing (like argparse or click), you can execute it from the command line (python demo_run.py) to quickly test the core module logic directly in your terminal without spinning up the server.

In conclusion: It's fundamentally a Python Module, wrapped in a REST API, which is then consumed by a Web Application.




### Input Field Definitions
All of the fields below are expected in the array of metric elements. While technically optional in code (it defaults to "Unknown" if missing), you must provide them for the Engine's clustering and drift algorithms to correctly function:

* **`metric_id`** *(string)*: 
Definition: A unique identifier for the specific metric instance. 

Purpose: Used by the engine to track exactly which metric is part of a duplicated cluster or has drifted. 

Example: "c01c0cc2" or "sales_rev_001"

* **`report_id`** *(string)*: The identifier or name of the source SSRS/BI report it originated from.
Definition: The name or identifier of the report, dashboard, or visual where this metric is displayed to the user. 

Purpose: Helps trace a bad or duplicated metric back to the exact dashboard where it lives so it can be fixed or retired. 

Example: "Sales Dashboard", "Executive Summary", or an SSRS report path like "/Sales/Weekly_Revenue_Rpt"


* **`dataset_id`** *(string)*: The identifier of the dataset within the report.
Definition: The name or identifier of the specific data model, dataset, or query that powers the visual. 

Purpose: Identifies the intermediate layer providing the data. In SSRS, this would be the specific <DataSet> name within the .rdl file. 

Example: "Core_Sales", "Agg_Users", or "DataSet1"

* **`metric_name`** *(string)*: The intended human-readable name of the KPI (e.g., "Sales Revenue").

Definition: The human-read business name of the measure being calculated. 

Purpose: The engine uses this (along with the calculation) to identify when two metrics are functionally identical but have different names, or have the exact same name but calculate the data differently (which causes confusion). 

Example: "Active Users", "Net Revenue", "Churn Rate"

* **`expression_signature`** *(string)*: The normalized formula, calculation, or Abstract Syntax Tree (AST) of the metric (e.g., `sum(amount)`).
Definition: The actual mathematical formula, DAX, or SQL aggregation used to compute the metric's value. 

Purpose: This is the most critical field. The AI compares these expressions to find metrics that are mathematically identical (duplication) or mathematically different when they shouldn't be (formula drift). 

Example: "sum(revenue)", "count_distinct(user_id)", or SSRS expressions like "=Sum(Fields!SalesAmount.Value)"

* **`grain`** *(string)*: The data aggregation level of the metric (e.g., `daily`, `monthly`, `account_level`). Drift in this field indicates Aggregation Drift.

Definition: The level of detail or time aggregation at which the metric is evaluated. 

Purpose: Used to detect Grain Drift. If one dashboard looks at "Revenue" by day and another looks at it by month, the engine flags this to ensure users don't compare mismatched numbers. 

Example: "day", "week", "month", "transaction_level"


* **`filters`** *(list of strings)*: An array of WHERE clauses or filter logic applied to the calculation. Differences here indicate Filter Drift.

Definition: A list of conditions, WHERE clauses, or parameters applied specifically to this metric to restrict the data it calculates. 

Purpose: Used to detect Filter Drift. If two "Revenue" metrics exist, but one has a filter ["region='US'"] and the other has ["is_active=TRUE"], the engine flags that these are actually calculating different things despite having the same name. 

Example: ["date >= '2023-01-01'"], ["plan='Premium'"], or SSRS parameters like ["@StartDate"]

* **`join_path_signature`** *(string)*: A representation of the database tables and joins calculating this data.

Definition: A string representing how the underlying tables were joined together to get the data for this metric. 

Purpose: Used to detect Join Drift. If two identical metrics calculate sum(revenue), but one gets there by joining sales->region and another by joining sales->account->region, the engine flags a risk that differing join logic might cause row explosion or dropped rows. 

Example: "users->sessions", "sales->region"


* **`source_objects`** *(list of strings)*: An array of strings representing the underlying database tables or views queried. *(Note: The Engine explicitly searches these names for the phrase "semantic view". If missing, it tags it as an ungoverned BI layer violation).*

Definition: The raw, foundational database tables or views that the metric reads from. 

Purpose: Identifies Architecture Violations. The engine checks if the metric is reading from raw, ungoverned tables (e.g., raw.events) rather than a governed semantic view (e.g., semantic.core). 

Example: ["raw.events"], ["dbo.FactSales"], ["sales_db.public.transactions"]


## output 

[
  {
    "metric_id": "m1",
    "report_id": "report_1",
    "dataset_id": "dataset_1",
    "metric_name": "Sales Revenue",
    "expression_signature": "sum(amount)",
    "grain": "daily",
    "filters": ["status='active'", "region='NA'"],
    "join_path_signature": "sales_table -> users_table",
    "source_objects": ["sales_table", "users_table"]
  },
  {
    "metric_id": "m2",
    "report_id": "report_2",
    "dataset_id": "dataset_1",
    "metric_name": "Sales Revenue",
    "expression_signature": "sum(amount)",
    "grain": "monthly",
    "filters": ["status='active'"],
    "join_path_signature": "sales_table -> users_table",
    "source_objects": ["sales_table", "users_table"]
  }
]

Here are the dependencies a user needs to install to run the project locally:

1. Python 3.7+
The project uses built-in standard libraries (http.server, 

json
, dataclasses, datetime, uuid, unittest etc.) but relies on feature syntaxes like @dataclass and from __future__ import annotations, meaning it requires a relatively modern version of Python 3.

2. Third-Party Dependencies
Depending on which parts of the project you intend to run, you will need to install the following via pip:

numpy: Required for the core accelerator engine (

src/accelerator/clustering.py
). This is mandatory if you want to run the web app (

app.py
), the REST API, or the CLI demo (

demo_run.py
).
pandas: Required only if you intend to run the 

extract_excel.py
 utility script.
openpyxl: Required by pandas for reading and extracting data from Excel workbooks (the project includes a tmp_libs folder with openpyxl and its dependencies, but it's best to install it natively to your environment).
You can install all necessary third-party dependencies with a single command:

bash
pip install numpy pandas openpyxl
Once installed, you can start the local web application by simply running:

bash
python app.py
