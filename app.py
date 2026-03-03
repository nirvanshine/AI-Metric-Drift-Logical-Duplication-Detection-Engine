import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from src.accelerator.models import MetricInstance
from src.accelerator.pipeline import run_analysis

HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>AI Metric Drift Detector</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 2rem; }
        .container { max-width: 800px; margin: 0 auto; background: #1e293b; padding: 2rem; border-radius: 12px; box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1); }
        h1 { color: #38bdf8; text-align: center; font-weight: 600; }
        .description { text-align: center; color: #94a3b8; margin-bottom: 2rem; }
        button { background: linear-gradient(to right, #0ea5e9, #2563eb); color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; font-weight: bold; width: 100%; transition: transform 0.2s, box-shadow 0.2s; box-shadow: 0 4px 6px -1px rgba(14, 165, 233, 0.4); }
        button:hover { transform: translateY(-2px); box-shadow: 0 6px 8px -1px rgba(14, 165, 233, 0.5); }
        button:active { transform: translateY(0); }
        pre { background: #020617; padding: 20px; border-radius: 8px; overflow-x: auto; color: #a5b4fc; font-family: 'Courier New', Courier, monospace; border: 1px solid #334155; }
        .metric-card { background: #334155; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 4px solid #38bdf8; }
        .metric-title { font-weight: bold; color: #f8fafc; }
        .metric-detail { color: #cbd5e1; font-size: 14px; margin-top: 5px; }
        h3 { color: #e2e8f0; border-bottom: 1px solid #334155; padding-bottom: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>AI Metric Drift Detector</h1>
        <p class="description">Dashboard for analyzing SSRS KPI duplication, metric drift, and BI layer violations.</p>
        
        <h3>Sample Metrics Loaded:</h3>
        <div id="metrics-list">
            <div class="metric-card">
                <div class="metric-title">Metric: Revenue (US)</div>
                <div class="metric-detail">Grain: day | Filters: region = 'US'</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Metric: Revenue (EMEA)</div>
                <div class="metric-detail">Grain: day | Filters: region = 'EMEA', status = 'closed'</div>
            </div>
            <div class="metric-card">
                <div class="metric-title">Metric: Revenue (APAC)</div>
                <div class="metric-detail">Grain: month | Filters: region = 'APAC'</div>
            </div>
        </div>

        <button onclick="runAnalysis()" id="run-btn">Run Engine Analysis</button>
        <div id="results" style="margin-top: 20px;"></div>
    </div>
    
    <script>
        async function runAnalysis() {
            const btn = document.getElementById('run-btn');
            const resultsDiv = document.getElementById('results');
            
            btn.disabled = true;
            btn.innerText = 'Analyzing...';
            resultsDiv.innerHTML = '<p style="text-align:center; color:#94a3b8;">Processing metrics through clustering and drift detection...</p>';
            
            try {
                const response = await fetch('/api/run', { method: 'POST' });
                const data = await response.json();
                resultsDiv.innerHTML = '<h3>Analysis Results</h3><pre>' + JSON.stringify(data, null, 2) + '</pre>';
            } catch (err) {
                resultsDiv.innerHTML = '<p style="color: #ef4444; background: #450a0a; padding: 15px; border-radius: 8px;">Error running analysis: ' + err.message + '</p>';
            } finally {
                btn.disabled = false;
                btn.innerText = 'Run Engine Analysis';
            }
        }
    </script>
</body>
</html>
"""

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/api/run':
            metrics = [
                MetricInstance("m1", "r1", "d1", "Revenue", "sum(revenue)", "day", ["region = 'US'"], "sales->region", ["raw.sales"]),
                MetricInstance("m2", "r2", "d2", "Revenue", "sum(revenue)", "day", ["region = 'EMEA'", "status = 'closed'"], "sales->region", ["raw.sales"]),
                MetricInstance("m3", "r3", "d3", "Revenue", "sum(revenue)", "month", ["region = 'APAC'"], "sales->region", ["raw.sales"])
            ]
            result = run_analysis(metrics)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(result).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

def run(server_class=HTTPServer, handler_class=SimpleHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting web server on http://localhost:{port}")
    httpd.serve_forever()

if __name__ == '__main__':
    run()
