import json
import logging
import os
import tempfile
from http.server import HTTPServer, BaseHTTPRequestHandler
from src.accelerator.models import MetricInstance
from src.accelerator.pipeline import run_analysis
from src.accelerator.export import generate_excel_pack

logging.basicConfig(level=logging.INFO)

# Load HTML from external file to avoid Python string quoting issues
_HTML_DIR = os.path.dirname(os.path.abspath(__file__))

# Module-level cache so /api/export can re-use the last run result
_last_result: dict = {}


class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            html_path = os.path.join(_HTML_DIR, 'index.html')
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(html_content.encode('utf-8'))
        elif self.path == '/api/export':
            self._handle_export()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == '/api/run':
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                self._send_error(400, "No payload provided")
                return

            post_data = self.rfile.read(content_length).decode('utf-8')
            try:
                raw_metrics = json.loads(post_data)

                metrics = []
                for idx, m in enumerate(raw_metrics):
                    metrics.append(MetricInstance(
                        metric_id=m.get("metric_id", f"m_{idx}"),
                        report_id=m.get("report_id", "default_report"),
                        dataset_id=m.get("dataset_id", "default_dataset"),
                        metric_name=m.get("metric_name", "Unknown"),
                        expression_signature=m.get("expression_signature", "unknown"),
                        grain=m.get("grain", "unknown"),
                        filters=m.get("filters", []),
                        join_path_signature=m.get("join_path_signature", "unknown"),
                        source_objects=m.get("source_objects", [])
                    ))

                result = run_analysis(metrics)

                # Cache for /api/export
                global _last_result
                _last_result = result

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(result).encode('utf-8'))

            except Exception as e:
                logging.error(f"Error processing metrics: {e}")
                self._send_error(500, str(e))

        elif self.path == '/api/export':
            # Also accept POST (e.g., with custom result payload)
            content_length = int(self.headers.get('Content-Length', 0))
            payload = {}
            if content_length:
                try:
                    payload = json.loads(self.rfile.read(content_length).decode('utf-8'))
                except Exception:
                    pass
            self._handle_export(payload or None)
        else:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length:
                self.rfile.read(content_length)
            self._send_error(404, "Not found")

    def _handle_export(self, result: dict | None = None):
        global _last_result
        data = result if result else _last_result
        if not data:
            self._send_error(400, "No analysis result available. Run /api/run first.")
            return

        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp_path = tmp.name

            generate_excel_pack(data, tmp_path)

            with open(tmp_path, "rb") as f:
                xlsx_bytes = f.read()
            os.unlink(tmp_path)

            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            self.send_header("Content-Disposition", 'attachment; filename="AI_Rationalization_Output_Pack.xlsx"')
            self.send_header("Content-Length", str(len(xlsx_bytes)))
            self.end_headers()
            self.wfile.write(xlsx_bytes)

        except RuntimeError as e:
            # openpyxl not installed
            self._send_error(501, str(e))
        except Exception as e:
            logging.error(f"Error generating Excel export: {e}")
            self._send_error(500, str(e))

    def _send_error(self, code, message):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({"error": message}).encode('utf-8'))

    def log_message(self, format, *args):
        pass


def run(server_class=HTTPServer, handler_class=SimpleHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting web server on http://localhost:{port}")
    httpd.serve_forever()


if __name__ == '__main__':
    run()
