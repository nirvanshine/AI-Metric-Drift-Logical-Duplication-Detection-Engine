from __future__ import annotations

import cgi
import html
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

from src.accelerator.demo_ingest import parse_report_upload
from src.accelerator.pipeline import run_analysis


HOST = "0.0.0.0"
PORT = 8501

SAMPLE_UPLOAD = {
    "reports": [
        {
            "report_id": "rpt_nav_trade",
            "name": "NAV Trade Date",
            "datasets": [
                {
                    "dataset_id": "ds_nav",
                    "sql_text": "SELECT SUM(nav) AS NAV FROM finance.positions WHERE trade_date = :as_of AND region = 'US'",
                }
            ],
        },
        {
            "report_id": "rpt_nav_settle",
            "name": "NAV Settle Date",
            "datasets": [
                {
                    "dataset_id": "ds_nav",
                    "sql_text": "SELECT SUM(nav) AS NAV FROM finance.positions WHERE settle_date = :as_of AND region = 'US'",
                }
            ],
        },
    ]
}


def render_page(result: dict | None = None, error: str | None = None) -> bytes:
    clusters = len(result["clusters"]) if result else 0
    findings = len(result["drift_findings"]) if result else 0
    recommendations = len(result["recommendations"]) if result else 0
    result_json = html.escape(json.dumps(result, indent=2)) if result else ""
    error_html = f"<p style='color:#b91c1c'><strong>Error:</strong> {html.escape(error)}</p>" if error else ""
    upload_template = html.escape(json.dumps(SAMPLE_UPLOAD, indent=2))

    body = f"""
    <!doctype html>
    <html>
    <head>
      <meta charset='utf-8' />
      <title>AI Rationalization Accelerator</title>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 2rem; max-width: 1200px; }}
        .cards {{ display: flex; gap: 1rem; margin: 1rem 0; }}
        .card {{ padding: 1rem; border: 1px solid #ddd; border-radius: 8px; min-width: 180px; }}
        button {{ background: #0f62fe; color: white; border: none; border-radius: 6px; padding: .6rem 1rem; cursor: pointer; }}
        pre {{ background: #f6f8fa; padding: 1rem; border-radius: 8px; overflow-x: auto; }}
      </style>
    </head>
    <body>
      <h1>AI-Driven Reporting Rationalization Accelerator</h1>
      <p>Upload SSRS report-spec JSON, then run duplication and drift detection.</p>
      <form method='post' enctype='multipart/form-data'>
        <input type='file' name='report_file' accept='.json' required />
        <button type='submit'>Upload & Run Analysis</button>
      </form>
      {error_html}
      <div class='cards'>
        <div class='card'><strong>Clusters</strong><br/>{clusters}</div>
        <div class='card'><strong>Drift Findings</strong><br/>{findings}</div>
        <div class='card'><strong>Recommendations</strong><br/>{recommendations}</div>
      </div>
      <h2>Expected Upload JSON Shape</h2>
      <pre>{upload_template}</pre>
      <h2>Analysis Output JSON</h2>
      <pre>{result_json}</pre>
    </body>
    </html>
    """
    return body.encode("utf-8")


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(render_page())

    def do_POST(self) -> None:  # noqa: N802
        try:
            form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={"REQUEST_METHOD": "POST"})
            file_item = form["report_file"] if "report_file" in form else None
            if file_item is None or not getattr(file_item, "file", None):
                response = render_page(error="Please upload a JSON file.")
            else:
                payload = file_item.file.read().decode("utf-8")
                metrics = parse_report_upload(payload)
                if not metrics:
                    response = render_page(error="No metrics detected in upload. Ensure SQL includes SUM/AVG/COUNT/MIN/MAX expressions.")
                else:
                    result = run_analysis(metrics)
                    response = render_page(result=result)
        except Exception as exc:  # noqa: BLE001
            response = render_page(error=str(exc))

        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(response)


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), AppHandler)
    print(f"Running accelerator app at http://{HOST}:{PORT}")
    server.serve_forever()
