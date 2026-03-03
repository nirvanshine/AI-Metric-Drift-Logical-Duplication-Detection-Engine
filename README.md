# AI-Metric-Drift-Logical-Duplication-Detection-Engine

Python accelerator scaffold for detecting SSRS KPI duplication, metric drift, and BI layer violations, then producing rationalization recommendations.

## What's Included

- Core domain entities for reports, metrics, clusters, drift findings, and recommendations.
- Weighted scoring models aligned to the PRD/TRD.
- Deterministic pipeline for clustering, drift detection, and recommendation actions.
- Browser app (`app.py`) that accepts a report-spec JSON upload and runs analysis.
- Demo ingestion utilities to parse uploaded report SQL and infer metric instances.
- Unit tests for scoring, pipeline, and upload ingestion behavior.

## Quick Start

> **Important path note:** `/workspace/...` is a Linux container path used in this development environment. If you are running locally on Windows, use your local filesystem path (for example `C:\Users\<you>\...`) instead.

### 1) Open the project folder in terminal

Linux/macOS (bash/zsh):

```bash
cd /path/to/AI-Metric-Drift-Logical-Duplication-Detection-Engine
```

Windows PowerShell:

```powershell
cd "C:\path\to\AI-Metric-Drift-Logical-Duplication-Detection-Engine"
```

Windows Command Prompt (cmd):

```cmd
cd /d C:\path\to\AI-Metric-Drift-Logical-Duplication-Detection-Engine
```

If needed, verify you are in the right folder:

```bash
python -c "import os; print(os.getcwd())"
```

### 2) Install dependencies (Windows local)

> The current demo app uses Python standard library modules only, so there are no mandatory third-party packages for runtime.

Create and activate a virtual environment (recommended):

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Upgrade pip and install optional dev/test dependencies (currently none required):

```powershell
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
```

If PowerShell blocks activation, run once as admin:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### 3) Run tests (optional but recommended)

Run tests:

```bash
python -m unittest discover -s tests
```

### 4) Run in browser

Run in browser:

```bash
python app.py
```

Then open `http://localhost:8501`.

### Common Windows error and fix

If you see this error in PowerShell:

```text
Cannot find path 'C:\workspace\AI-Metric-Drift-Logical-Duplication-Detection-Engine' because it does not exist
```

it means you are using the container path on your local machine. Use your real local clone path instead:

```powershell
cd "C:\Users\<your-user>\Downloads\AI-Metric-Drift-Logical-Duplication-Detection-Engine"
python app.py
```

## Demo: Upload Report and Surface Duplication

1. Start the app (`python app.py`).
2. In browser, upload a JSON file with this shape:

```json
{
  "reports": [
    {
      "report_id": "rpt_nav_trade",
      "datasets": [
        {
          "dataset_id": "ds_nav",
          "sql_text": "SELECT SUM(nav) AS NAV FROM finance.positions WHERE trade_date = :as_of"
        }
      ]
    },
    {
      "report_id": "rpt_nav_settle",
      "datasets": [
        {
          "dataset_id": "ds_nav",
          "sql_text": "SELECT SUM(nav) AS NAV FROM finance.positions WHERE settle_date = :as_of"
        }
      ]
    }
  ]
}
```

3. Click **Upload & Run Analysis**.
4. Review output counters and JSON for detected clusters (duplication) and drift findings.

Use in code:

```python
from src.accelerator import run_analysis
```

See implementation notes in `docs/accelerator_blueprint.md`.


## Ready-to-use Demo Upload Files

Use any of these files directly in the upload box:

- `demo_data/duplicate_nav_reports.json`
- `demo_data/duplicate_return_reports.json`
- `demo_data/mixed_kpis.json`

Quick demo link after starting app:

- `http://localhost:8501`

Start the app:

```bash
python app.py
```
