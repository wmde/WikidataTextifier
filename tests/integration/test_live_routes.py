"""Live integration tests against the local FastAPI service."""

import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pymysql
import pytest

pytestmark = pytest.mark.integration
LOCAL_BASE_URL = "http://127.0.0.1:5000"


def _api_get(path: str, params: dict | None = None, expected_status: int | None = 200) -> dict:
    """Submit a GET request to the local API and return parsed response data."""
    query = f"?{urlencode(params or {}, doseq=True)}" if params else ""
    req = Request(
        f"{LOCAL_BASE_URL}{path}{query}",
        method="GET",
        headers={
            "User-Agent": "Pytest Integration Suite/1.0 (integration-tests@example.org)",
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(req, timeout=120) as res:
            status = res.status
            body_bytes = res.read()
            headers = dict(res.headers.items())
    except HTTPError as e:
        status = e.code
        body_bytes = e.read()
        headers = dict(e.headers.items()) if e.headers else {}
    except URLError as e:
        pytest.fail(f"Local API is unreachable at {LOCAL_BASE_URL}: {e}")

    body_text = body_bytes.decode("utf-8", errors="replace")
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError:
        payload = body_text

    if expected_status is not None:
        assert status == expected_status, f"{path} expected {expected_status}, got {status}: {payload}"

    return {"status": status, "payload": payload, "headers": headers}


def _load_env_file() -> dict[str, str]:
    """Load key-value pairs from local ``.env`` file if present."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    out: dict[str, str] = {}
    if not env_path.exists():
        return out

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_value = value.strip()
        if (
            len(normalized_value) >= 2
            and normalized_value[0] == normalized_value[-1]
            and normalized_value[0] in {"'", '"'}
        ):
            normalized_value = normalized_value[1:-1]
        out[key.strip()] = normalized_value
    return out


def _db_config() -> dict[str, str | int]:
    """Build DB connection config from environment with sensible defaults."""
    env_file = _load_env_file()

    user = os.environ.get("DB_USER") or env_file.get("DB_USER", "root")
    password = os.environ.get("DB_PASS")
    if password is None:
        password = env_file.get("DB_PASS", "")

    db_name = os.environ.get("DB_NAME")
    if db_name is None:
        db_name = env_file.get("DB_NAME_LABEL") or env_file.get("DB_NAME", "label")

    return {
        "host": os.environ.get("DB_HOST") or env_file.get("DB_HOST", "127.0.0.1"),
        "port": int(os.environ.get("DB_PORT") or env_file.get("DB_PORT", "3306")),
        "user": user,
        "password": password,
        "database": db_name,
    }


def _db_connect():
    """Open a DB connection for cache verification queries."""
    cfg = _db_config()
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        autocommit=True,
    )


def test_docs_route_is_reachable():
    """Validate docs route is reachable."""
    result = _api_get("/docs", expected_status=200)
    content_type = result["headers"].get("Content-Type") or result["headers"].get("content-type", "")
    assert "text/html" in content_type


def test_entity_query_json_contract_for_multi_ids():
    """Validate JSON contract for multi-ID query."""
    result = _api_get(
        "/",
        params={
            "id": "Q42,Q2",
            "format": "json",
            "lang": "en",
            "pid": "P31",
        },
        expected_status=200,
    )
    payload = result["payload"]

    assert isinstance(payload, dict)
    assert set(payload.keys()) == {"Q42", "Q2"}
    assert isinstance(payload["Q42"], dict)
    assert payload["Q42"]["QID"] == "Q42"
    assert "claims" in payload["Q42"]


def test_entity_query_text_contract_for_single_id():
    """Validate text contract for single-ID query."""
    result = _api_get(
        "/",
        params={
            "id": "Q42",
            "format": "text",
            "lang": "en",
            "pid": "P31",
        },
        expected_status=200,
    )
    payload = result["payload"]

    assert isinstance(payload, dict)
    assert "Q42" in payload
    assert isinstance(payload["Q42"], str)
    assert payload["Q42"]


def test_cache_writes_and_reuses_label_entries():
    """Validate label cache rows are written and then reused across repeated requests."""
    tracked_ids = ["P31", "Q5"]

    try:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM labels WHERE id IN (%s, %s)",
                    (tracked_ids[0], tracked_ids[1]),
                )
    except pymysql.err.OperationalError as e:
        pytest.skip(f"Cannot connect to MariaDB for cache verification: {e}")

    first = _api_get(
        "/",
        params={
            "id": "Q42,Q2",
            "format": "json",
            "lang": "en",
            "pid": "P31",
        },
        expected_status=200,
    )
    assert isinstance(first["payload"], dict)

    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, date_added FROM labels WHERE id IN (%s, %s)",
                (tracked_ids[0], tracked_ids[1]),
            )
            rows_first = cur.fetchall()

    assert rows_first, "Expected label cache entries to be created after first request."
    first_dates = {row[0]: row[1] for row in rows_first}
    assert "P31" in first_dates

    second = _api_get(
        "/",
        params={
            "id": "Q42,Q2",
            "format": "json",
            "lang": "en",
            "pid": "P31",
        },
        expected_status=200,
    )
    assert isinstance(second["payload"], dict)

    with _db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, date_added FROM labels WHERE id IN (%s, %s)",
                (tracked_ids[0], tracked_ids[1]),
            )
            rows_second = cur.fetchall()

    second_dates = {row[0]: row[1] for row in rows_second}
    assert second_dates["P31"] == first_dates["P31"]
