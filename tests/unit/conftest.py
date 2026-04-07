"""Setup for unit tests: shared fixtures and import bootstrap."""

import asyncio
import sys
from pathlib import Path
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def run_async():
    """Run an async coroutine in unit tests."""

    def _run(coro):
        return asyncio.run(coro)

    return _run


@pytest.fixture
def make_request():
    """Create a minimal Starlette request object for route calls."""

    def _make(path: str, method: str = "GET", params: dict | None = None) -> Request:
        """Construct a request scope with query params and test headers."""
        query_string = urlencode(params or {}, doseq=True).encode()
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "path": path,
            "query_string": query_string,
            "headers": [
                (b"user-agent", b"Unit Test Client/1.0 (unit-tests@example.org)"),
            ],
            "client": ("127.0.0.1", 12345),
            "scheme": "http",
            "server": ("testserver", 80),
        }
        return Request(scope)

    return _make
