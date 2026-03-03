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



## Quick Start

Run tests:

```bash
python -m unittest discover -s tests
```

Use in code:

```python
from src.accelerator import run_analysis
```

See implementation notes in `docs/accelerator_blueprint.md`.
