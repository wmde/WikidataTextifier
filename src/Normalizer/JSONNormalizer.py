"""Normalize Wikidata/Wikibase Action API JSON into internal textifier objects."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

from ..Textifier.WikidataTextifier import (
    WikidataClaim,
    WikidataClaimValue,
    WikidataCoordinates,
    WikidataEntity,
    WikidataQuantity,
    WikidataText,
    WikidataTime,
)
from ..utils import wikidata_geolocation_to_text, wikidata_time_to_text
from ..WikidataLabel import LazyLabelFactory, WikidataLabel


class JSONNormalizer:
    """Normalize ``wbgetentities`` JSON into internal textifier objects."""

    def __init__(
        self,
        entity_id: str,
        entity_json: Dict[str, Any],
        lang: str = "en",
        fallback_lang: str = "en",
        label_factory: Optional[LazyLabelFactory] = None,
        debug: bool = False,
    ):
        """Initialize a normalizer for a single entity payload.

        Args:
            entity_id (str): Entity ID being normalized.
            entity_json (dict[str, Any]): Raw ``wbgetentities`` JSON for ``entity_id``.
            lang (str): Preferred language for label selection.
            fallback_lang (str): Fallback language when ``lang`` is unavailable.
            label_factory (LazyLabelFactory | None): Shared lazy label factory for nested entities.
            debug (bool): Whether to print additional debug output while parsing.
        """
        self.entity_id = entity_id
        self.entity_json = entity_json

        self.lang = lang
        self.fallback_lang = fallback_lang
        self.debug = debug

        self.label_factory = label_factory or LazyLabelFactory(lang=lang, fallback_lang=fallback_lang)

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def normalize(
        self,
        external_ids: bool = True,
        references: bool = False,
        all_ranks: bool = False,
        qualifiers: bool = True,
        filter_pids: List[str] = [],
    ) -> WikidataEntity:
        """Normalize the entity JSON payload into a ``WikidataEntity`` tree.

        Args:
            external_ids (bool): Whether to include ``external-id`` datatype claims.
            references (bool): Whether to include references for each statement value.
            all_ranks (bool): Whether to include statements of all ranks.
            qualifiers (bool): Whether to include qualifiers for statement values.
            filter_pids (list[str]): Optional allow-list of property IDs to keep.

        Returns:
            WikidataEntity: Parsed entity object with claims and values.
        """
        e = self.entity_json
        if not isinstance(e, dict) or "labels" not in e:
            if self.debug:
                print(f"Bad entity_json: missing labels for item {self.entity_id}")

        label = WikidataLabel.get_lang_val(
            e.get("labels", {}) or {},
            lang=self.lang,
            fallback_lang=self.fallback_lang,
        )
        description = WikidataLabel.get_lang_val(
            e.get("descriptions", {}) or {},
            lang=self.lang,
            fallback_lang=self.fallback_lang,
        )

        aliases_dict = e.get("aliases", {}) or {}
        aliases = (aliases_dict.get(self.lang, []) or []) + (aliases_dict.get("mul", []) or [])
        aliases = list({a.get("value") if isinstance(a, dict) else str(a) for a in aliases if a})

        entity = WikidataEntity(
            id=self.entity_id,
            label=label,
            description=description,
            aliases=aliases,
            claims=[],
        )

        claims_in = e.get("claims", {}) or {}
        claims_out: List[WikidataClaim] = []
        for pid, statements in claims_in.items():
            if not (isinstance(pid, str) and pid.startswith("P") and isinstance(statements, list)):
                continue

            if filter_pids and pid not in filter_pids:
                continue

            claim_obj = self._build_claim(
                subject=entity,
                pid=pid,
                statements=statements,
                external_ids=external_ids,
                include_references=references,
                all_ranks=all_ranks,
                qualifiers=qualifiers,
            )
            if claim_obj is not None and claim_obj.values:
                claims_out.append(claim_obj)

        entity.claims = claims_out

        # keep the same circular backreference behavior as your existing code
        for c in entity.claims:
            c.subject = entity

        return entity

    # -------------------------------------------------------------------------
    # Claim building
    # -------------------------------------------------------------------------

    def _build_claim(
        self,
        *,
        subject: WikidataEntity,
        pid: str,
        statements: List[Dict[str, Any]],
        external_ids: bool,
        include_references: bool,
        all_ranks: bool,
        qualifiers: bool,
    ) -> Optional[WikidataClaim]:
        datatype = self._claim_datatype_from_statements(statements) or "string"
        if (not external_ids) and datatype == "external-id":
            return None

        prop_ent = WikidataEntity(
            id=pid,
            label=self.label_factory.create(pid),
            description=None,
            aliases=[],
            claims=[],
        )
        claim = WikidataClaim(subject=subject, property=prop_ent, values=[], datatype=datatype)

        kept = self._filter_by_rank(statements, all_ranks=all_ranks)

        values: List[WikidataClaimValue] = []
        for st in kept:
            if not isinstance(st, dict):
                continue
            cv = self._build_claim_value(
                claim=claim,
                statement=st,
                datatype=datatype,
                include_references=include_references,
                qualifiers=qualifiers,
            )
            if cv is not None:
                values.append(cv)

        claim.values = values
        return claim

    def _claim_datatype_from_statements(self, statements: List[Dict[str, Any]]) -> Optional[str]:
        for st in statements:
            if not isinstance(st, dict):
                continue
            ms = st.get("mainsnak", st)
            if not isinstance(ms, dict):
                continue
            dt = ms.get("datatype")
            if isinstance(dt, str) and dt:
                return dt
        return None

    def _filter_by_rank(self, statements: List[Dict[str, Any]], *, all_ranks: bool) -> List[Dict[str, Any]]:
        if all_ranks:
            return [s for s in statements if isinstance(s, dict)]

        has_preferred = any(isinstance(s, dict) and s.get("rank") == "preferred" for s in statements)

        kept: List[Dict[str, Any]] = []
        for s in statements:
            if not isinstance(s, dict):
                continue
            r = s.get("rank")
            if r is None:
                kept.append(s)
            elif has_preferred and r == "preferred":
                kept.append(s)
            elif (not has_preferred) and r == "normal":
                kept.append(s)

        return kept

    def _build_claim_value(
        self,
        *,
        claim: WikidataClaim,
        statement: Dict[str, Any],
        datatype: str,
        include_references: bool,
        qualifiers: bool,
    ) -> Optional[WikidataClaimValue]:
        mainsnak = statement.get("mainsnak", statement)
        if not isinstance(mainsnak, dict):
            return None

        snaktype = mainsnak.get("snaktype", "value")
        if snaktype != "value":
            # somevalue/novalue
            return WikidataClaimValue(
                claim=claim,
                value=None,
                qualifiers=[],
                references=[],
                rank=statement.get("rank"),
            )

        datavalue = mainsnak.get("datavalue")
        value_obj = self._to_value_object(datatype, datavalue)

        qualifiers_obj: List[WikidataClaim] = []
        if qualifiers:
            qualifiers_obj = self._parse_qualifiers(statement.get("qualifiers", {}) or {})
        references_obj: List[List[WikidataClaim]] = []
        if include_references:
            references_obj = self._parse_references(statement.get("references", []) or [])

        if self.debug:
            print(f"{claim.property.id}: {datavalue} (snaktype={snaktype})")

        return WikidataClaimValue(
            claim=claim,
            value=value_obj,
            qualifiers=qualifiers_obj,
            references=references_obj,
            rank=statement.get("rank"),
        )

    # -------------------------------------------------------------------------
    # Qualifiers and references
    # -------------------------------------------------------------------------

    def _parse_qualifiers(self, qualifiers: Dict[str, Any]) -> List[WikidataClaim]:
        out: List[WikidataClaim] = []
        for qpid, snaks in qualifiers.items():
            if not (isinstance(qpid, str) and qpid.startswith("P") and isinstance(snaks, list)):
                continue
            out.append(self._build_snak_claim(pid=qpid, snaks=snaks, dummy_subject_id="<qualifier>"))
        return out

    def _parse_references(self, references: List[Dict[str, Any]]) -> List[List[WikidataClaim]]:
        out: List[List[WikidataClaim]] = []
        for ref in references:
            if not isinstance(ref, dict):
                continue
            snaks = ref.get("snaks", {}) or {}
            if not isinstance(snaks, dict):
                continue

            ref_claims: List[WikidataClaim] = []
            for rpid, r_snaks in snaks.items():
                if not (isinstance(rpid, str) and rpid.startswith("P") and isinstance(r_snaks, list)):
                    continue
                ref_claims.append(self._build_snak_claim(pid=rpid, snaks=r_snaks, dummy_subject_id="<reference>"))

            out.append(ref_claims)
        return out

    def _build_snak_claim(self, *, pid: str, snaks: List[Dict[str, Any]], dummy_subject_id: str) -> WikidataClaim:
        prop_ent = WikidataEntity(
            id=pid,
            label=self.label_factory.create(pid),
            description=None,
            aliases=[],
            claims=[],
        )
        dummy_subject = WikidataEntity(id=dummy_subject_id, label=None, description=None, aliases=[], claims=[])

        datatype = self._datatype_from_snaks(snaks) or "string"
        claim = WikidataClaim(subject=dummy_subject, property=prop_ent, values=[], datatype=datatype)

        vals: List[WikidataClaimValue] = []
        for snak in snaks:
            if not isinstance(snak, dict):
                continue
            snaktype = snak.get("snaktype", "value")
            if snaktype != "value":
                vals.append(WikidataClaimValue(claim=claim, value=None, qualifiers=[], references=[], rank=None))
                continue

            dv = snak.get("datavalue")
            vobj = self._to_value_object(datatype, dv)
            vals.append(WikidataClaimValue(claim=claim, value=vobj, qualifiers=[], references=[], rank=None))

        claim.values = vals
        return claim

    def _datatype_from_snaks(self, snaks: List[Dict[str, Any]]) -> Optional[str]:
        for s in snaks:
            if not isinstance(s, dict):
                continue
            dt = s.get("datatype")
            if isinstance(dt, str) and dt:
                return dt
        return None

    # -------------------------------------------------------------------------
    # Datavalue -> model objects
    # -------------------------------------------------------------------------

    def _to_value_object(
        self,
        datatype: str,
        datavalue: Any,
    ) -> object:
        if not isinstance(datavalue, dict):
            return None

        dv_type = datavalue.get("type")
        dv_val = datavalue.get("value")

        # Entity
        if dv_type == "wikibase-entityid" or datatype in ("wikibase-item", "wikibase-property"):
            eid = None
            if isinstance(dv_val, dict):
                eid = dv_val.get("id")
            if isinstance(eid, str) and eid.startswith(("Q", "P")):
                return WikidataEntity(
                    id=eid,
                    label=self.label_factory.create(eid),
                    description=None,
                    aliases=[],
                    claims=[],
                )
            return WikidataText(text=str(dv_val))

        # Time
        if dv_type == "time" or datatype == "time":
            if not isinstance(dv_val, dict):
                return None
            time_val = dv_val.get("time")
            if not isinstance(time_val, str) or not time_val:
                return None

            calendarmodel = dv_val.get("calendarmodel") or "http://www.wikidata.org/entity/Q1985786"
            cal_id = calendarmodel.rsplit("/", 1)[-1] if isinstance(calendarmodel, str) else "Q1985786"

            try:
                string_val = wikidata_time_to_text(
                    dv_val,
                    self.lang,
                )
            except (ValueError, TypeError, KeyError, requests.RequestException) as e:
                if self.debug:
                    print(f"Warning: Failed to parse time value {time_val}: {e}")
                return None

            return WikidataTime(
                time=time_val,
                precision=dv_val.get("precision"),
                calendarmodel=cal_id,
                string_val=string_val,
            )

        # Quantity
        if dv_type == "quantity" or datatype == "quantity":
            if not isinstance(dv_val, dict):
                return None
            amount = dv_val.get("amount")
            if amount is None:
                return None

            unit_uri = dv_val.get("unit")
            unit_id = None
            unit_label = None
            if isinstance(unit_uri, str) and unit_uri != "1":
                unit_id = unit_uri.rsplit("/", 1)[-1] if "/" in unit_uri else unit_uri
                if unit_id.startswith("Q"):
                    unit_label = self.label_factory.create(unit_id)

            return WikidataQuantity(amount=str(amount), unit=unit_label, unit_id=unit_id)

        if dv_type == "globe-coordinate" or datatype == "globe-coordinate":
            if not isinstance(dv_val, dict):
                return None
            lat = dv_val.get("latitude")
            lon = dv_val.get("longitude")
            if lat is None or lon is None:
                return None

            try:
                string_val = wikidata_geolocation_to_text(dv_val, self.lang)
            except (ValueError, TypeError, KeyError, requests.RequestException) as e:
                if self.debug:
                    print(f"Warning: Failed to parse coordinates ({lat}, {lon}): {e}")
                return None

            return WikidataCoordinates(latitude=lat, longitude=lon, string_val=string_val)

        # Monolingual text
        if dv_type == "monolingualtext" or datatype == "monolingualtext":
            if not isinstance(dv_val, dict):
                return None
            txt = dv_val.get("text")
            lg = dv_val.get("language")
            if lg != self.lang:
                return WikidataText(text=None)
            return WikidataText(text=str(txt) if txt is not None else "")

        # Default string-like
        return WikidataText(text=str(dv_val) if dv_val is not None else None)
