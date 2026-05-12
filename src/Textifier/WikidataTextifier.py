"""Data structures for Wikidata entities and serialization helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

LANGUAGE_VARIABLES_PATH = Path(__file__).with_name("language_variables.json")
with LANGUAGE_VARIABLES_PATH.open("r", encoding="utf-8") as f:
    LANGUAGE_VARIABLES = json.load(f)

# ---------------------------------------------------------------------------
# Atomic value types
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class WikidataText:
    """Object for Wikidata plain text values."""

    text: Optional[str] = None

    def __str__(self) -> str:
        """Return the text representation."""
        return self.text or ""

    def __bool__(self) -> bool:
        """Return whether this text wrapper contains content."""
        return bool(self.text)

    def to_json(self) -> Optional[str]:
        """Serialize to a JSON-friendly scalar."""
        return self.text


@dataclass(slots=True)
class WikidataMonolingualText:
    """Object for Wikidata monolingual text values."""

    text: Optional[str] = None
    lang: Optional[str] = None

    def __str__(self) -> str:
        """Return the text representation."""
        return self.lang + ":" + self.text if self.lang and self.text else self.text or ""

    def __bool__(self) -> bool:
        """Return whether this text wrapper contains content."""
        return bool(self.text)

    def to_json(self) -> Optional[str]:
        """Serialize to a JSON object."""
        return {
            "text": self.text,
            "lang": self.lang
        }


@dataclass(slots=True)
class WikidataCoordinates:
    """Object for Wikidata coordinate values."""

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    string_val: Optional[str] = None

    def __str__(self) -> str:
        """Return a readable coordinate string."""
        return self.string_val or "lat: {}, lon: {}".format(self.latitude, self.longitude)

    def __bool__(self) -> bool:
        """Return whether both latitude and longitude are present."""
        # coordinates are meaningful if we have both lat/lon
        return (
            self.latitude is not None
            and self.longitude is not None
        )

    def to_json(self) -> Dict[str, Any]:
        """Serialize coordinates to a JSON object."""
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "string": self.string_val,
        }


@dataclass(slots=True)
class WikidataTime:
    """Object for Wikidata time values."""

    time: Optional[str] = None
    precision: Optional[int] = None
    calendarmodel: Optional[str] = None
    string_val: Optional[str] = None

    def __str__(self) -> str:
        """Return a readable time string."""
        return self.string_val or str(self.time)

    def __bool__(self) -> bool:
        """Return whether this instance contains a time value."""
        return bool(self.time) or bool(self.string_val)

    def to_json(self) -> Dict[str, Any]:
        """Serialize time to a JSON object."""
        return {
            "time": self.time,
            "precision": self.precision,
            "calendar_QID": self.calendarmodel,
            "string": self.string_val,
        }


@dataclass(slots=True)
class WikidataQuantity:
    """Object for Wikidata quantity values."""

    amount: Optional[str] = None
    unit: Optional[Any] = None
    unit_id: Optional[str] = None

    def __str__(self) -> str:
        """Return a readable quantity string."""
        if not self.amount:
            return ""
        if self.unit_id:
            return f"{self.amount} {str(self.unit)}"
        return str(self.amount)

    def __bool__(self) -> bool:
        """Return whether this quantity has an amount."""
        return bool(self.amount)

    def to_json(self) -> Any:
        """Serialize quantity to a scalar or object."""
        if not self.amount:
            return None
        if self.unit_id:
            return {
                "amount": self.amount,
                "unit": str(self.unit),
                "unit_QID": self.unit_id,
            }
        return self.amount


# ---------------------------------------------------------------------------
# Core graph types
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class WikidataEntity:
    """Object for Wikidata entities."""

    id: str
    label: Optional[Any] = None
    description: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    claims: List["WikidataClaim"] = field(default_factory=list)

    def __bool__(self) -> bool:
        """Return whether this entity has a usable id and label."""
        return (
            bool(self.id)
            and self.label is not None
            and str(self.label) != ""
        )

    def to_text(self, lang="en") -> str:
        """Render the entity into a readable text."""
        lang_var = LANGUAGE_VARIABLES.get(lang, LANGUAGE_VARIABLES.get("en"))

        label_str = str(self.label) if self.label else "<no label>"
        string = label_str

        if self.description:
            string += f"{lang_var[', ']}{self.description}"
        if self.aliases:
            string += f"{lang_var[', ']}{lang_var['also known as']}"
            string += f" {lang_var[', '].join(map(str, self.aliases))}"

        attributes = [c.to_text(lang) for c in self.claims]
        attributes= [a for a in attributes if a]  # filter out empty attributes

        if len(attributes) > 0:
            attributes = "\n- ".join(attributes)
            string += f". {lang_var['Attributes include']}:\n- {attributes}"
        elif string != label_str:
            string += "."

        return string

    def to_json(self) -> Dict[str, Any]:
        """Serialize the entity to a JSON object."""
        id_key = "PID" if self.id.startswith("P") else "QID"
        return {
            id_key: self.id,
            "label": str(self.label) if self.label else None,
            "description": self.description if self.description else None,
            "aliases": self.aliases,
            "claims": [c.to_json() for c in self.claims],
        }

    def to_triplet(self) -> str:
        """Render the entity as triplet lines."""
        head = f"{str(self.label) if self.label else '<missing>'} ({self.id})"
        lines: List[str] = []
        if self.description:
            lines.append(f"description: {self.description}")
        if self.aliases:
            lines.append(f"aliases: {', '.join(map(str, self.aliases))}")

        claims = [c.to_triplet() for c in self.claims]
        claims = [c for c in claims if c]  # filter out empty claims
        lines.extend(claims)

        if not lines:
            return head

        exploded = "\n".join(lines).split("\n")
        return "\n".join(f"{head}: {line}" for line in exploded)


@dataclass(slots=True)
class WikidataClaim:
    """Object for Wikidata claims."""

    subject: WikidataEntity
    property: WikidataEntity
    values: List["WikidataClaimValue"] = field(default_factory=list)
    datatype: str = "string"

    def __bool__(self) -> bool:
        """Return whether this claim contains a value."""
        return (
            bool(self.property)
            and any(bool(v) for v in self.values)
        )

    def to_text(self, lang="en") -> str:
        """Render the claim into a readable text."""
        lang_var = LANGUAGE_VARIABLES.get(lang, LANGUAGE_VARIABLES.get("en"))

        # For text format, remove claims with missing property label
        # TODO: Consider replacing missing label with <no label> instead of removing the claim entirely.
        if not bool(self.property):
            return ""

        if any(bool(v) for v in self.values):
            values = lang_var[", "].join(v.to_text(lang) for v in self.values if v)
            return f"{str(self.property.label)}: {values}"

        # if no values, show existence of the property.
        return f"{lang_var['has']} {str(self.property.label)}"

    def to_json(self) -> Dict[str, Any]:
        """Serialize the claim to a JSON object."""
        prop_json = self.property.to_json()
        prop_id = prop_json.get("PID") or prop_json.get("QID")

        values = [v.to_json() for v in self.values]
        values = [v for v in values if v]  # filter out empty values

        return {
            "PID": prop_id,
            "property_label": prop_json["label"],
            "datatype": self.datatype,
            "values": values,
        }

    def to_triplet(self, as_qualifier: bool = False) -> str:
        """Render the claim as triplet text."""
        prop_label = str(self.property.label) if bool(self.property) else "<no label>"

        # For triplet format, keep claims with missing property label
        label = f"{prop_label} ({self.property.id})"

        value_lines = [v.to_triplet() for v in self.values]
        value_lines = [v for v in value_lines if v]  # filter out empty values

        # Remove claims with missing values
        if not value_lines:
            return ""

        if as_qualifier:
            # qualifier: multiple values on one line
            return f"{label}: {', '.join(value_lines)}"

        # main claim: one line per value
        return "\n".join(f"{label}: {v}" for v in value_lines)


@dataclass(slots=True)
class WikidataClaimValue:
    """Object for Wikidata claim values."""

    claim: WikidataClaim
    value: Optional[
        Union[
            WikidataEntity, WikidataQuantity, WikidataTime, WikidataCoordinates, WikidataText, WikidataMonolingualText
            ]
        ] = None
    qualifiers: List[WikidataClaim] = field(default_factory=list)
    references: List[List[WikidataClaim]] = field(default_factory=list)
    rank: Optional[str] = None  # preferred|normal|deprecated

    def __bool__(self) -> bool:
        """Return whether this claim value has non-empty values."""
        return bool(self.value)

    def to_text(self, lang="en") -> str:
        """Render the value and qualifiers as readable text."""
        lang_var = LANGUAGE_VARIABLES.get(lang, LANGUAGE_VARIABLES.get("en"))

        # TODO: Consider showing qualifiers even if the main value is missing
        if not self:
            return ""

        if isinstance(self.value, WikidataEntity):
            s = self.value.to_text(lang)
        else:
            s = str(self.value)

        if self.rank == "deprecated":
            s += " [deprecated]"

        qs = [q.to_text(lang) for q in self.qualifiers]
        qs = [q for q in qs if q]  # filter out empty qualifiers
        if qs:
            s += f" ({lang_var[', '].join(qs)})"

        return s

    def to_json(self) -> Optional[Dict[str, Any]]:
        """Serialize the claim value to a JSON object."""
        # value serialization
        if hasattr(self.value, "to_json"):
            value_json = self.value.to_json()
        else:
            value_json = str(self.value)

        if isinstance(self.value, WikidataEntity) and isinstance(value_json, dict):
            id_name = "QID" if self.claim.datatype == "wikibase-item" else "PID"
            entity_id = value_json.get("QID") or value_json.get("PID")
            value_json = {
                id_name: entity_id,
                "label": str(value_json.get("label")),
            }

        if not value_json:
            return None

        out: Dict[str, Any] = {"value": value_json}

        if self.qualifiers:
            qualifiers = [q.to_json() for q in self.qualifiers]
            out["qualifiers"] = [q for q in qualifiers if q]  # filter out empty qualifiers

        if self.references:
            out["references"] = [[r.to_json() for r in ref] for ref in self.references]

        if self.rank:
            out["rank"] = self.rank

        return out

    def to_triplet(self) -> str:
        """Render the value as triplet text."""
        s = None
        if isinstance(self.value, WikidataEntity):
            label = str(self.value.label) if bool(self.value) else "<no label>"
            s = f"{label} ({self.value.id})"
        else:
            s = str(self.value)

        # TODO: Consider showing qualifiers even if the main value is missing
        if not s:
            return ""

        if self.rank == "deprecated":
            s += " [deprecated]"

        q_lines = [q.to_triplet(as_qualifier=True) for q in self.qualifiers]
        q_lines = [q for q in q_lines if q]  # filter out empty
        if q_lines:
            s += f" | {' | '.join(q_lines)}"
        return s
