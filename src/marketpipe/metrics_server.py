from prometheus_client import start_http_server

def run(port: int = 8000) -> None:
    start_http_server(port=port)
