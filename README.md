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

How it is Executed
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
