from prometheus_client import start_http_server

def start_metrics_server(port: int = 8000):
    start_http_server(port)
    print(f"[Metrics] Prometheus metrics available at http://localhost:{port}/metrics")
