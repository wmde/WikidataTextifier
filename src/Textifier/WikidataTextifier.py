from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import json

LANGUAGE_VARIABLES_PATH = Path(__file__).with_name("language_variables.json")
with LANGUAGE_VARIABLES_PATH.open("r", encoding="utf-8") as f:
    LANGUAGE_VARIABLES = json.load(f)

# ---------------------------------------------------------------------------
# Atomic value types
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class WikidataText:
    text: Optional[str] = None

    def __str__(self) -> str:
        return self.text or ""

    def __bool__(self) -> bool:
        return bool(self.text)

    def to_json(self) -> Optional[str]:
        return self.text


@dataclass(slots=True)
class WikidataCoordinates:
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    string_val: Optional[str] = None

    def __str__(self) -> str:
        return self.string_val or ""

    def __bool__(self) -> bool:
        # coordinates are meaningful if we have both lat/lon
        return self.latitude is not None and self.longitude is not None

    def to_json(self) -> Dict[str, Any]:
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "string": self.string_val,
        }


@dataclass(slots=True)
class WikidataTime:
    time: Optional[str] = None
    precision: Optional[int] = None
    calendarmodel: Optional[str] = None
    string_val: Optional[str] = None

    def __str__(self) -> str:
        return self.string_val or ""

    def __bool__(self) -> bool:
        return bool(self.time) or bool(self.string_val)

    def to_json(self) -> Dict[str, Any]:
        return {
            "time": self.time,
            "precision": self.precision,
            "calendar_QID": self.calendarmodel,
            "string": self.string_val,
        }


@dataclass(slots=True)
class WikidataQuantity:
    amount: Optional[str] = None
    unit: Optional[Any] = None
    unit_id: Optional[str] = None

    def __str__(self) -> str:
        if not self.amount:
            return ""
        if self.unit_id:
            return f"{self.amount} {str(self.unit)}"
        return str(self.amount)

    def __bool__(self) -> bool:
        return bool(self.amount)

    def to_json(self) -> Any:
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
    id: str
    label: Optional[Any] = None
    description: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    claims: List["WikidataClaim"] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.id) and self.label is not None and str(self.label) != ""

    def to_text(self, lang='en', keep_empty: bool = False) -> str:
        lang_var = LANGUAGE_VARIABLES.get(lang, LANGUAGE_VARIABLES.get('en'))

        label_str = str(self.label) if self.label else '<missing>'
        string = label_str

        if self.description:
            string += f"{lang_var[', ']}{self.description}"
        if self.aliases:
            string += f"{lang_var[', ']}{lang_var['also known as']}"
            string += f" {lang_var[', '].join(map(str, self.aliases))}"

        attributes = [c.to_text(lang, keep_empty=keep_empty) \
                      for c in self.claims \
                        if keep_empty or c]
        if len(attributes) > 0:
            attributes = "\n- ".join(attributes)
            string += f". {lang_var['Attributes include']}:\n- {attributes}"
        elif string != label_str:
            string += "."

        return string

    def to_json(self) -> Dict[str, Any]:
        id_key = "PID" if self.id.startswith("P") else "QID"
        return {
            id_key: self.id,
            "label": str(self.label) if self.label else None,
            "description": self.description,
            "aliases": self.aliases,
            "claims": [c.to_json() for c in self.claims if c],
        }

    def to_triplet(self) -> str:
        head = f"{str(self.label) if self.label else '<missing>'} ({self.id})"
        lines: List[str] = []
        if self.description:
            lines.append(f"description: {self.description}")
        if self.aliases:
            lines.append(f"aliases: {', '.join(map(str, self.aliases))}")
        lines.extend([c.to_triplet() for c in self.claims if c])

        if not lines:
            return head

        exploded: List[str] = "\n".join(lines).split("\n")
        return "\n".join(f"{head}: {line}" for line in exploded)


@dataclass(slots=True)
class WikidataClaim:
    subject: WikidataEntity
    property: WikidataEntity
    values: List["WikidataClaimValue"] = field(default_factory=list)
    datatype: str = "string"

    def __bool__(self) -> bool:
        return (
            self.property is not None
            and str(self.property.label) != ""
            and len(self.values) > 0
            and any(bool(v) for v in self.values)
        )

    def to_text(self, lang='en') -> str:
        lang_var = LANGUAGE_VARIABLES.get(lang, LANGUAGE_VARIABLES.get('en'))

        if self.values:
            values = lang_var[', '].join(v.to_text(lang) for v in self.values if v)
            return f"{str(self.property.label)}: {values}"

        return f"{lang_var['has']} {str(self.property.label)}"

    def to_json(self) -> Dict[str, Any]:
        prop_json = self.property.to_json()
        prop_id = prop_json.get("PID") or prop_json.get("QID")
        return {
            "PID": prop_id,
            "property_label": prop_json["label"],
            "datatype": self.datatype,
            "values": [v.to_json() for v in self.values if v],
        }

    def to_triplet(self, as_qualifier: bool = False) -> str:
        if not self:
            return ""

        label = f"{str(self.property.label)} ({self.property.id})"
        value_lines = [v.to_triplet() for v in self.values if v]

        if not value_lines:
            return ""

        if as_qualifier:
            # qualifier: multiple values on one line
            return f"{label}: {', '.join(value_lines)}"

        # main claim: one line per value
        return "\n".join(f"{label}: {v}" for v in value_lines)


@dataclass(slots=True)
class WikidataClaimValue:
    claim: WikidataClaim
    value: Optional[
        Union[WikidataEntity, WikidataQuantity, WikidataTime, WikidataCoordinates, WikidataText]
    ] = None
    qualifiers: List[WikidataClaim] = field(default_factory=list)
    references: List[List[WikidataClaim]] = field(default_factory=list)
    rank: Optional[str] = None  # preferred|normal|deprecated

    def __bool__(self) -> bool:
        return self.value is not None and str(self.value) != ""

    def to_text(self, lang='en') -> str:
        lang_var = LANGUAGE_VARIABLES.get(lang, LANGUAGE_VARIABLES.get('en'))

        if not self:
            return ""

        if isinstance(self.value, WikidataEntity):
            s = self.value.to_text(lang)
        else:
            s = str(self.value)

        if self.rank == "deprecated":
            s += " [deprecated]"

        qs = [q.to_text(lang) for q in self.qualifiers if q]
        if qs:
            s += f" ({lang_var[', '].join(qs)})"

        return s

    def to_json(self) -> Optional[Dict[str, Any]]:
        if not self:
            return None

        # value serialization
        if hasattr(self.value, "to_json"):
            value_json = self.value.to_json()
        else:
            value_json = str(self.value)

        # If the value is an entity, normalize its JSON shape like your original logic.
        if isinstance(self.value, WikidataEntity) and isinstance(value_json, dict):
            id_name = "QID" if self.claim.datatype == "wikibase-item" else "PID"
            entity_id = value_json.get("QID") or value_json.get("PID")
            value_json = {
                id_name: entity_id,
                "label": str(value_json.get("label")),
            }

        out: Dict[str, Any] = {"value": value_json}

        if self.qualifiers:
            out["qualifiers"] = [q.to_json() for q in self.qualifiers if q]

        if self.references:
            out["references"] = [[r.to_json() for r in ref if r] for ref in self.references]

        if self.rank:
            out["rank"] = self.rank

        return out

    def to_triplet(self) -> str:
        if not self:
            return ""

        s = str(self.value)
        if isinstance(self.value, WikidataEntity):
            s = f"{str(self.value.label)} ({self.value.id})"

        if self.rank == "deprecated":
            s += " [deprecated]"

        q_lines = [q.to_triplet(as_qualifier=True) for q in self.qualifiers if q]
        if q_lines:
            s += f" | {' | '.join(q_lines)}"
        return s
