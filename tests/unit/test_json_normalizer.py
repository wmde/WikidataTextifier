"""Unit tests for JSON normalizer behavior.

Covers claim filtering and datavalue conversion behavior for key datatypes.
"""

from importlib import import_module

from src.Normalizer.JSONNormalizer import JSONNormalizer
from src.Textifier.WikidataTextifier import WikidataQuantity, WikidataText

json_normalizer_module = import_module("src.Normalizer.JSONNormalizer")


class _DummyLabelFactory:
    """Minimal label factory that records requested IDs."""

    def __init__(self):
        self.requested_ids = []

    def create(self, qid):
        """Return a stable synthetic label for an ID."""
        self.requested_ids.append(qid)
        return f"label-{qid}"


def _base_entity_json():
    """Create a minimal entity payload with labels/descriptions/aliases."""
    return {
        "labels": {"en": {"value": "Douglas Adams"}},
        "descriptions": {"en": {"value": "English writer"}},
        "aliases": {"en": [{"value": "DNA"}]},
        "claims": {},
    }


def test_normalize_filters_external_id_claims_when_disabled():
    """It should exclude ``external-id`` datatype claims when ``external_ids=False``."""
    data = _base_entity_json()
    data["claims"] = {
        "P31": [
            {
                "rank": "normal",
                "mainsnak": {
                    "snaktype": "value",
                    "datatype": "wikibase-item",
                    "datavalue": {"type": "wikibase-entityid", "value": {"id": "Q5"}},
                },
            }
        ],
        "P214": [
            {
                "rank": "normal",
                "mainsnak": {
                    "snaktype": "value",
                    "datatype": "external-id",
                    "datavalue": {"type": "string", "value": "113230702"},
                },
            }
        ],
    }

    normalizer = JSONNormalizer(
        entity_id="Q42",
        entity_json=data,
        label_factory=_DummyLabelFactory(),
    )
    entity = normalizer.normalize(external_ids=False)

    assert len(entity.claims) == 1
    assert entity.claims[0].property.id == "P31"


def test_filter_by_rank_prefers_preferred_statements():
    """It should keep preferred statements (plus rank-less ones) when present."""
    normalizer = JSONNormalizer(
        entity_id="Q42",
        entity_json=_base_entity_json(),
        label_factory=_DummyLabelFactory(),
    )
    statements = [
        {"rank": "normal"},
        {"rank": "preferred"},
        {"rank": None},
    ]

    filtered = normalizer._filter_by_rank(statements, all_ranks=False)

    assert len(filtered) == 2
    assert {"rank": "preferred"} in filtered
    assert {"rank": None} in filtered


def test_to_value_object_quantity_resolves_unit_label():
    """It should map quantity unit URIs to unit IDs and lazy labels."""
    factory = _DummyLabelFactory()
    normalizer = JSONNormalizer(
        entity_id="Q42",
        entity_json=_base_entity_json(),
        label_factory=factory,
    )

    quantity = normalizer._to_value_object(
        "quantity",
        {
            "type": "quantity",
            "value": {
                "amount": "+10",
                "unit": "http://www.wikidata.org/entity/Q11573",
            },
        },
    )

    assert isinstance(quantity, WikidataQuantity)
    assert quantity.unit_id == "Q11573"
    assert quantity.unit == "label-Q11573"
    assert "Q11573" in factory.requested_ids


def test_to_value_object_monolingual_text_ignores_other_languages():
    """It should return empty monolingual text when language does not match target ``lang``."""
    normalizer = JSONNormalizer(
        entity_id="Q42",
        entity_json=_base_entity_json(),
        lang="en",
        label_factory=_DummyLabelFactory(),
    )

    value = normalizer._to_value_object(
        "monolingualtext",
        {"type": "monolingualtext", "value": {"text": "Bonjour", "language": "fr"}},
    )

    assert isinstance(value, WikidataText)
    assert value.text is None


def test_to_value_object_time_returns_none_when_formatter_fails(monkeypatch):
    """It should return ``None`` for time values when formatter call fails."""

    def fake_time_formatter(value, lang):
        raise ValueError("cannot format")

    monkeypatch.setattr(json_normalizer_module, "wikidata_time_to_text", fake_time_formatter)

    normalizer = JSONNormalizer(
        entity_id="Q42",
        entity_json=_base_entity_json(),
        label_factory=_DummyLabelFactory(),
        debug=False,
    )

    value = normalizer._to_value_object(
        "time",
        {
            "type": "time",
            "value": {
                "time": "+2024-01-01T00:00:00Z",
                "precision": 11,
                "calendarmodel": "http://www.wikidata.org/entity/Q1985786",
            },
        },
    )

    assert value is None
