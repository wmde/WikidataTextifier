"""Unit tests for utility helpers.

Covers HTTP helper wiring, chunking behavior, and formatter error handling.
"""

import json

import pytest

from src import utils


class _FakeResponse:
    """Simple fake response object for mocked HTTP calls."""

    def __init__(self, *, payload=None, text=""):
        self._payload = payload or {}
        self.text = text

    def raise_for_status(self):
        """Mimic successful HTTP responses."""

    def json(self):
        """Return JSON payload."""
        return self._payload


def test_get_wikidata_ttl_by_id_returns_response_text(monkeypatch):
    """It should return the raw TTL text for a requested entity."""

    def fake_get(url, params, headers, timeout):
        return _FakeResponse(text="ttl-content")

    monkeypatch.setattr(utils.SESSION, "get", fake_get)

    result = utils.get_wikidata_ttl_by_id("Q42", lang="en")

    assert result == "ttl-content"


def test_get_wikidata_json_by_ids_deduplicates_and_chunks(monkeypatch):
    """It should deduplicate IDs and split requests in chunks of 50."""
    captured_chunks = []

    def fake_get(url, params, headers, timeout):
        chunk_ids = params["ids"].split("|")
        captured_chunks.append(chunk_ids)
        entities = {qid: {"id": qid} for qid in chunk_ids}
        return _FakeResponse(payload={"entities": entities})

    monkeypatch.setattr(utils.SESSION, "get", fake_get)

    ids = [f"Q{i}" for i in range(1, 55)] + ["Q1"]  # 54 unique IDs
    result = utils.get_wikidata_json_by_ids(ids)

    assert len(captured_chunks) == 2
    assert len(captured_chunks[0]) == 50
    assert len(captured_chunks[1]) == 4
    assert len(result) == 54
    assert "Q1" in result
    assert "Q54" in result


def test_wikidata_time_to_text_normalizes_time_before_api_call(monkeypatch):
    """It should normalize time payloads before posting to formatter API."""
    captured = {}

    def fake_post(url, data, timeout):
        captured["datavalue"] = json.loads(data["datavalue"])
        return _FakeResponse(payload={"result": "1 January 2024"})

    monkeypatch.setattr(utils.SESSION, "post", fake_post)

    result = utils.wikidata_time_to_text({"time": "2024-01-01T00:00:00+00:00"}, lang="en")

    assert result == "1 January 2024"
    assert captured["datavalue"]["value"]["time"] == "+2024-01-01T00:00:00Z"


def test_wikidata_time_to_text_raises_for_missing_result(monkeypatch):
    """It should raise ``ValueError`` when formatter response has no result field."""

    def fake_post(url, data, timeout):
        return _FakeResponse(payload={"error": "missing result"})

    monkeypatch.setattr(utils.SESSION, "post", fake_post)

    with pytest.raises(ValueError):
        utils.wikidata_time_to_text({"time": "+2024-01-01T00:00:00Z"}, lang="en")


def test_wikidata_geolocation_to_text_raises_for_missing_result(monkeypatch):
    """It should raise ``ValueError`` when coordinate formatter response is malformed."""

    def fake_post(url, data, timeout):
        return _FakeResponse(payload={"error": "missing result"})

    monkeypatch.setattr(utils.SESSION, "post", fake_post)

    with pytest.raises(ValueError):
        utils.wikidata_geolocation_to_text({"latitude": 1.0, "longitude": 2.0}, lang="en")
