# AI-Metric-Drift-Logical-Duplication-Detection-Engine

Python accelerator scaffold for detecting SSRS KPI duplication, metric drift, and BI layer violations, then producing rationalization recommendations.

## What's Included

- Core domain entities for reports, metrics, clusters, drift findings, and recommendations.
- Weighted scoring models aligned to the PRD/TRD.
- Deterministic pipeline for:
  - clustering metrics
  - detecting drift types (formula/filter/join/grain)
  - generating recommendation actions
- Unit tests for scoring and pipeline behavior.

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
