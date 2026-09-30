"""CORS regression using only liveness, with no startup side effects."""
import asyncio
import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
import main


class CorsTests(unittest.TestCase):
    def test_only_local_frontend_passes_preflight(self):
        async def preflight(origin):
            messages = []
            async def receive():
                return {"type": "http.request", "body": b""}
            async def send(message):
                messages.append(message)
            await main.app({"type": "http", "http_version": "1.1", "scheme": "http",
                "method": "OPTIONS", "path": "/health/live", "query_string": b"",
                "headers": [(b"origin", origin.encode()),
                            (b"access-control-request-method", b"GET")]}, receive, send)
            return messages[0]

        for origin, expected in [("http://localhost:5400", 200),
                                 ("http://127.0.0.1:5400", 200),
                                 ("https://untrusted.example", 400)]:
            response = asyncio.run(preflight(origin))
            self.assertEqual(response["status"], expected)
            self.assertEqual(dict(response["headers"]).get(b"access-control-allow-origin"),
                             origin.encode() if expected == 200 else None)
