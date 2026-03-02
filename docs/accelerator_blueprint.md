# AI-Driven Reporting Rationalization Accelerator Blueprint

This repository now includes a Python accelerator scaffold aligned to the provided PRD/TRD and user journey.

## Implemented Modules

1. **Canonical domain model** (`src/accelerator/models.py`)
   - Captures Report, Dataset, MetricInstance, MetricCluster, DriftFinding, Recommendation, and run summary structures.
2. **Scoring models** (`src/accelerator/scoring.py`)
   - Drift Severity Score (40/25/20/15 weighting).
   - Consolidation Priority Score (30/20/20/15/15 weighting).
3. **Recommendation engine** (`src/accelerator/recommendation.py`)
   - Action routing for Standardize / Consolidate / Retire / Rebuild / Move-to-Snowflake.
4. **Pipeline orchestrator** (`src/accelerator/pipeline.py`)
   - Metric clustering, drift detection, explainable findings, and recommendation generation.
5. **Demo upload ingestion** (`src/accelerator/demo_ingest.py`)
   - Parses uploaded report-spec JSON and extracts metric instances from SQL aggregations for browser demos.

## Design Notes

- The implementation is deliberately deterministic and rule-based so that teams can validate explainability before introducing embedding models.
- `run_analysis()` is the current entrypoint for local batch processing and can be wrapped later with FastAPI.
- Drift explanations are plain-English and directly tied to detected differences in filters, joins, formulas, and grain.

## Next Build Steps

- Add SSRS RDL parser and ReportServer ingest connector.
- Add SQL AST/canonicalization (SQLGlot) to populate expression and join signatures.
- Add Snowflake metadata resolver and lineage overlay.
- Add export pack generator (Excel/PDF/CSV/Jira payload).
