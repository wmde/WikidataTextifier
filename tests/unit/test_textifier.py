"""Unit tests for textifier model behavior.

Covers truthiness rules, serialization, and triplet/text rendering behavior.
"""

from src.Textifier.WikidataTextifier import (
    WikidataClaim,
    WikidataClaimValue,
    WikidataCoordinates,
    WikidataEntity,
    WikidataQuantity,
)


def test_wikidata_coordinates_bool_requires_lat_and_lon():
    """It should only be truthy when both latitude and longitude are set."""
    assert not WikidataCoordinates(latitude=1.0, longitude=None)
    assert bool(WikidataCoordinates(latitude=1.0, longitude=2.0))


def test_wikidata_quantity_string_and_json_with_unit():
    """It should include the unit label/id in string and JSON output."""
    quantity = WikidataQuantity(amount="+10", unit="metre", unit_id="Q11573")

    assert str(quantity) == "+10 metre"
    assert quantity.to_json() == {
        "amount": "+10",
        "unit": "metre",
        "unit_QID": "Q11573",
    }


def test_wikidata_entity_to_text_includes_description_and_aliases():
    """It should render label, description, and aliases in text format."""
    entity = WikidataEntity(
        id="Q42",
        label="Douglas Adams",
        description="English writer",
        aliases=["DNA"],
        claims=[],
    )

    rendered = entity.to_text(lang="en")

    assert "Douglas Adams" in rendered
    assert "English writer" in rendered
    assert "DNA" in rendered


def test_claim_value_entity_serialization_uses_qid_for_wikibase_item():
    """It should serialize entity values with ``QID`` when claim datatype is ``wikibase-item``."""
    subject = WikidataEntity(id="Q42", label="Douglas Adams")
    prop = WikidataEntity(id="P31", label="instance of")
    claim = WikidataClaim(subject=subject, property=prop, datatype="wikibase-item")

    value_entity = WikidataEntity(id="Q5", label="human")
    claim_value = WikidataClaimValue(claim=claim, value=value_entity)
    claim.values = [claim_value]

    result = claim_value.to_json()

    assert result == {"value": {"QID": "Q5", "label": "human"}}


def test_claim_to_triplet_renders_one_line_per_value():
    """It should render one triplet line per claim value."""
    subject = WikidataEntity(id="Q42", label="Douglas Adams")
    prop = WikidataEntity(id="P31", label="instance of")
    claim = WikidataClaim(subject=subject, property=prop, datatype="wikibase-item")
    claim.values = [
        WikidataClaimValue(claim=claim, value=WikidataEntity(id="Q5", label="human")),
        WikidataClaimValue(claim=claim, value=WikidataEntity(id="Q215627", label="person")),
    ]

    rendered = claim.to_triplet()

    assert "instance of (P31): human (Q5)" in rendered
    assert "instance of (P31): person (Q215627)" in rendered
