import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlparse


class LocalRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # extract the callback query
        self.server.last_query = SimpleNamespace(
            **dict(parse_qsl(urlparse(self.path).query))
        )

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(
            bytes(
                """<html><head><title>Deploy Script Databricks Login</title></head>
                <body>
                <h>Deploy Script Databricks Login</h>
                <p>You can close this tab.</p>
                </body></html>
                """,
                "utf-8",
            )
        )

    def log_message(self, format, *args):
        """We don't want the output that someone connected to us,
        because the query contains a secret"""
        print("Callback received.", file=sys.stderr)


class LocalServer(HTTPServer):
    def __init__(self):
        super().__init__(("localhost", 0), LocalRequestHandler)
        self.timeout = 60
