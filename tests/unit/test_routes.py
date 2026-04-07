"""Unit tests for API routes.

Covers route wiring behavior for single and multi-entity requests.
"""

from fastapi import BackgroundTasks

import main
from src.Textifier.WikidataTextifier import WikidataEntity


def test_get_textified_wd_uses_ttl_normalizer_for_single_qid(monkeypatch, run_async, make_request):
    """Validate ``TTLNormalizer`` is used when one QID is requested."""
    calls = {}

    def fake_get_ttl(qid, lang="en"):
        calls["requested_qid"] = qid
        return "ttl-data"

    class DummyTTLNormalizer:
        """Minimal TTL normalizer stand-in for unit testing."""

        def __init__(self, **kwargs):
            self.entity_id = kwargs["entity_id"]
            calls["normalizer_entity_id"] = self.entity_id

        def normalize(self, **kwargs):
            return WikidataEntity(id=self.entity_id, label="Douglas Adams", claims=[])

    monkeypatch.setattr(main.utils, "get_wikidata_ttl_by_id", fake_get_ttl)
    monkeypatch.setattr(main, "TTLNormalizer", DummyTTLNormalizer)

    result = run_async(
        main.get_textified_wd(
            request=make_request("/"),
            background_tasks=BackgroundTasks(),
            id="Q42",
            pid=None,
            format="json",
        )
    )

    assert calls["requested_qid"] == "Q42"
    assert calls["normalizer_entity_id"] == "Q42"
    assert result["Q42"]["QID"] == "Q42"
    assert result["Q42"]["label"] == "Douglas Adams"


def test_get_textified_wd_uses_json_normalizer_for_multiple_qids(monkeypatch, run_async, make_request):
    """Validate ``JSONNormalizer`` is used for multi-QID requests."""
    init_calls = []

    def fake_get_json(ids):
        return {
            "Q1": {"labels": {"en": {"value": "One"}}, "descriptions": {}, "aliases": {}, "claims": {}},
            "Q2": {"labels": {"en": {"value": "Two"}}, "descriptions": {}, "aliases": {}, "claims": {}},
        }

    class DummyJSONNormalizer:
        """Minimal JSON normalizer stand-in for unit testing."""

        def __init__(self, **kwargs):
            self.entity_id = kwargs["entity_id"]
            init_calls.append(self.entity_id)

        def normalize(self, **kwargs):
            return WikidataEntity(id=self.entity_id, label=f"Label-{self.entity_id}", claims=[])

    monkeypatch.setattr(main.utils, "get_wikidata_json_by_ids", fake_get_json)
    monkeypatch.setattr(main, "JSONNormalizer", DummyJSONNormalizer)

    result = run_async(
        main.get_textified_wd(
            request=make_request("/"),
            background_tasks=BackgroundTasks(),
            id="Q1,Q2",
            pid=None,
            format="text",
        )
    )

    assert init_calls == ["Q1", "Q2"]
    assert result["Q1"] == "Label-Q1"
    assert result["Q2"] == "Label-Q2"
