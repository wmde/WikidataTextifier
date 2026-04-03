"""Normalize Wikidata TTL into internal textifier objects."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Set

import requests
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS

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

# Namespaces used by Wikidata TTL
WD = Namespace("http://www.wikidata.org/entity/")
P = Namespace("http://www.wikidata.org/prop/")
PS = Namespace("http://www.wikidata.org/prop/statement/")
PSV = Namespace("http://www.wikidata.org/prop/statement/value/")
PQ = Namespace("http://www.wikidata.org/prop/qualifier/")
PQV = Namespace("http://www.wikidata.org/prop/qualifier/value/")
PQN = Namespace("http://www.wikidata.org/prop/qualifier/value-normalized/")
PR = Namespace("http://www.wikidata.org/prop/reference/")
PRV = Namespace("http://www.wikidata.org/prop/reference/value/")
PRN = Namespace("http://www.wikidata.org/prop/reference/value-normalized/")

WIKIBASE = Namespace("http://wikiba.se/ontology#")
SCHEMA = Namespace("http://schema.org/")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
PROV = Namespace("http://www.w3.org/ns/prov#")


class TTLNormalizer:
    """Normalize ``Special:EntityData`` TTL into internal textifier objects.

    Label resolution order:
        1) Labels present in TTL.
        2) ``LazyLabelFactory`` bulk lookup for unresolved IDs.

    Notes:
        - Claims are extracted from ``wd:<Q> p:<P> <statement-node>`` triples only.
        - Statement nodes are validated structurally before value extraction.
        - Special values (somevalue/novalue) are treated as "no main value" when
        neither ps:<pid> nor psv:<pid> is present on the statement node.
        - Property datatype is read from ``wikibase:propertyType`` when available,
        otherwise inferred from the statement's value nodes when possible.
    """

    def __init__(
        self,
        entity_id: str,
        ttl_text: str,
        lang: str = "en",
        fallback_lang: str = "en",
        label_factory: Optional[LazyLabelFactory] = None,
        debug: bool = False,
    ):
        """Initialize a normalizer for a single TTL document.

        Args:
            entity_id (str): Entity ID being normalized.
            ttl_text (str): Raw TTL document from ``Special:EntityData``.
            lang (str): Preferred language for label selection.
            fallback_lang (str): Fallback language when ``lang`` is unavailable.
            label_factory (LazyLabelFactory | None): Shared lazy label factory for nested entities.
            debug (bool): Whether to print additional debug output while parsing.
        """
        self.entity_id = entity_id
        self.g = Graph()
        self.g.parse(data=ttl_text, format="turtle")

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
        """Normalize the parsed graph into a ``WikidataEntity`` tree.

        Args:
            external_ids (bool): Whether to include ``external-id`` datatype claims.
            references (bool): Whether to include references for each statement value.
            all_ranks (bool): Whether to include statements of all ranks.
            qualifiers (bool): Whether to include qualifiers for statement values.
            filter_pids (list[str]): Optional allow-list of property IDs to keep.

        Returns:
            WikidataEntity: Parsed entity object with claims and values.
        """
        # Preload labels found inside TTL so LazyLabelFactory can avoid lookups.
        self.label_factory._resolved_labels = self._build_label_cache_from_ttl()

        subj = WD[self.entity_id]

        label = WikidataLabel.get_lang_val(
            self._lang_value_map(subj, RDFS.label),
            lang=self.lang,
            fallback_lang=self.fallback_lang,
        )
        description = WikidataLabel.get_lang_val(
            self._lang_value_map(subj, SCHEMA.description),
            lang=self.lang,
            fallback_lang=self.fallback_lang,
        )

        aliases_map = self._aliases_lang_map(subj, SKOS.altLabel)
        aliases = (aliases_map.get(self.lang, []) or []) + (aliases_map.get("mul", []) or [])
        aliases = list({a["value"] if isinstance(a, dict) else str(a) for a in aliases if a})

        claims_dict = self._claims_for_subject(
            subj,
            external_ids=external_ids,
            include_references=references,
            all_ranks=all_ranks,
            qualifiers=qualifiers,
            filter_pids=filter_pids,
        )

        entity = WikidataEntity(
            id=self.entity_id,
            label=label,
            description=description,
            aliases=aliases,
            claims=[],
        )

        entity.claims = [
            self._build_claim_object(
                subject=entity,
                pid=pid,
                statements=statements,
                include_references=references,
                qualifiers=qualifiers,
            )
            for pid, statements in claims_dict.items()
            if statements
        ]
        return entity

    # -------------------------------------------------------------------------
    # Label cache
    # -------------------------------------------------------------------------

    def _build_label_cache_from_ttl(self) -> Dict[str, Dict[str, Any]]:
        """Cache labels for any wd:Qxxx or wd:Pxxx that appear as RDF subjects."""
        label_cache: Dict[str, Dict[str, Any]] = {}
        candidates: Set[str] = set()

        for s in self.g.subjects():
            sid = self._qid_from_wd_uri(s)
            if sid and (sid.startswith("Q") or sid.startswith("P")):
                candidates.add(sid)

        for sid in candidates:
            node = WD[sid]
            labels = self._lang_value_map(node, RDFS.label)
            if labels:
                label_cache[sid] = {"labels": labels}

        return WikidataLabel._compress_labels(label_cache)

    # -------------------------------------------------------------------------
    # Claim extraction
    # -------------------------------------------------------------------------

    def _claims_for_subject(
        self,
        subj: URIRef,
        external_ids: bool,
        include_references: bool,
        all_ranks: bool,
        qualifiers: bool,
        filter_pids: List[str] = [],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Return mapping: pid -> list of statement dicts."""
        out: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)

        for pred, obj in self.g.predicate_objects(subj):
            if not (isinstance(pred, URIRef) and isinstance(obj, URIRef)):
                continue
            if not str(pred).startswith(str(P)):
                continue

            pid = self._pid_from_prop_uri(pred)
            if not pid:
                continue

            if filter_pids and pid not in filter_pids:
                continue

            if not self._is_statement_node(obj, pid):
                if self.debug:
                    print(f"Skipping non-statement object for {pid}: {obj}")
                continue

            datatype = self._prop_datatype(pid, statement_node=obj)
            if (not external_ids) and datatype == "external-id":
                continue

            rank_uri = self.g.value(obj, WIKIBASE.rank)
            rank = self._rank_to_str(rank_uri) if isinstance(rank_uri, URIRef) else None

            is_special = self._is_special_main_value(obj, pid)
            main = None if is_special else self._main_value(obj, pid, datatype)

            qualifiers_data = self._qualifiers(obj) if qualifiers else {}
            refs = self._references(obj) if include_references else []

            out[pid].append(
                {
                    "pid": pid,
                    "datatype": datatype,
                    "rank": rank,
                    "main": main,
                    "qualifiers": qualifiers_data if qualifiers_data else {},
                    "references": refs if refs else [],
                    "is_special_value": is_special,
                }
            )

        if all_ranks:
            return dict(out)

        # Rank filtering, preserved exactly:
        # - if any preferred exists: keep preferred only (plus rank None)
        # - else keep normal only (plus rank None)
        filtered: Dict[str, List[Dict[str, Any]]] = {}
        for pid, sts in out.items():
            has_preferred = any(s.get("rank") == "preferred" for s in sts)
            keep: List[Dict[str, Any]] = []
            for s in sts:
                r = s.get("rank")
                if r is None:
                    keep.append(s)
                elif has_preferred and r == "preferred":
                    keep.append(s)
                elif (not has_preferred) and r == "normal":
                    keep.append(s)
            filtered[pid] = keep
        return filtered

    # -------------------------------------------------------------------------
    # Statement node detection and special values
    # -------------------------------------------------------------------------

    def _is_statement_node(self, node: URIRef, pid: str) -> bool:
        """Structural check that this node behaves like a Wikibase statement node."""
        if not isinstance(node, URIRef):
            return False

        if self.g.value(node, WIKIBASE.rank) is not None:
            return True

        # Most robust: statement must carry ps:<pid> or psv:<pid> for its main snak
        if self.g.value(node, PS[pid]) is not None or self.g.value(node, PSV[pid]) is not None:
            return True

        if self.g.value(node, RDF.type) == WIKIBASE.Statement:
            return True

        # Fallback: any predicate in ps/psv namespaces indicates a statement-shaped node.
        for p, _ in self.g.predicate_objects(node):
            if isinstance(p, URIRef):
                sp = str(p)
                if sp.startswith(str(PS)) or sp.startswith(str(PSV)):
                    return True
        return False

    def _is_special_main_value(self, statement_node: URIRef, pid: str) -> bool:
        """Treat as special when the statement node has no ps:<pid> and no psv:<pid>."""
        return self.g.value(statement_node, PS[pid]) is None and self.g.value(statement_node, PSV[pid]) is None

    # -------------------------------------------------------------------------
    # Build model objects
    # -------------------------------------------------------------------------

    def _build_claim_object(
        self,
        *,
        subject: WikidataEntity,
        pid: str,
        statements: List[Dict[str, Any]],
        include_references: bool,
        qualifiers: bool = True,
    ) -> WikidataClaim:
        prop_ent = WikidataEntity(
            id=pid,
            label=self.label_factory.create(pid),
            description=None,
            aliases=[],
            claims=[],
        )

        datatype = statements[0].get("datatype") or "string"
        claim = WikidataClaim(subject=subject, property=prop_ent, values=[], datatype=datatype)

        values: List[WikidataClaimValue] = []
        for st in statements:
            if self.debug:
                print(f"{pid}: {st.get('main')} (special: {st.get('is_special_value', False)})")

            value_obj = self._to_value_object(st["datatype"], st.get("main"))
            qualifiers_obj: List[WikidataClaim] = []

            if qualifiers:
                qualifiers_obj = [
                    self._build_snak_claim(
                        pid=qpid,
                        datatype=self._prop_datatype(qpid),
                        snaks=qsnaks,
                    )
                    for qpid, qsnaks in (st.get("qualifiers") or {}).items()
                ]

            refs_obj: List[List[WikidataClaim]] = []
            if include_references:
                for ref in st.get("references") or []:
                    ref_claims = [
                        self._build_snak_claim(
                            pid=rpid,
                            datatype=self._prop_datatype(rpid),
                            snaks=rsnaks,
                        )
                        for rpid, rsnaks in (ref.get("snaks") or {}).items()
                    ]
                    refs_obj.append(ref_claims)

            values.append(
                WikidataClaimValue(
                    claim=claim,
                    value=value_obj,
                    qualifiers=qualifiers_obj,
                    references=refs_obj,
                    rank=st.get("rank"),
                )
            )

        claim.values = values
        return claim

    def _build_snak_claim(self, *, pid: str, datatype: str, snaks: List[Dict[str, Any]]) -> WikidataClaim:
        prop_ent = WikidataEntity(
            id=pid,
            label=self.label_factory.create(pid),
            description=None,
            aliases=[],
            claims=[],
        )

        dummy_subject = WikidataEntity(id="<snak>", label=None, description=None, aliases=[], claims=[])
        claim = WikidataClaim(subject=dummy_subject, property=prop_ent, values=[], datatype=datatype)

        vals: List[WikidataClaimValue] = []
        for snak in snaks:
            if self.debug:
                print(f"  {pid}: {snak.get('value')}")
            v_obj = self._to_value_object(datatype, snak.get("value"))
            vals.append(WikidataClaimValue(claim=claim, value=v_obj, qualifiers=[], references=[], rank=None))

        claim.values = vals
        return claim

    # -------------------------------------------------------------------------
    # Converting parsed values to model objects
    # -------------------------------------------------------------------------

    def _to_value_object(self, datatype: str, parsed: Any) -> Any:
        if parsed is None:
            return None

        if datatype == "wikibase-item":
            if isinstance(parsed, str) and parsed.startswith("Q"):
                return WikidataEntity(
                    id=parsed,
                    label=self.label_factory.create(parsed),
                    description=None,
                    aliases=[],
                    claims=[],
                )
            return WikidataText(text=str(parsed))

        if datatype == "quantity":
            if not isinstance(parsed, dict):
                if self.debug:
                    print(f"Warning: Expected dict for quantity, got {type(parsed)}")
                return None
            amount = parsed.get("amount")
            if amount is None:
                if self.debug:
                    print(f"Warning: Quantity missing amount: {parsed}")
                return None

            unit_uri = parsed.get("unit")
            unit_id: Optional[str] = None
            unit_label: Optional[Any] = None
            if isinstance(unit_uri, str) and unit_uri != "1":
                unit_id = unit_uri.rsplit("/", 1)[-1] if "/" in unit_uri else unit_uri
                if unit_id.startswith("Q"):
                    unit_label = self.label_factory.create(unit_id)

            return WikidataQuantity(amount=amount, unit=unit_label, unit_id=unit_id)

        if datatype == "time":
            if not isinstance(parsed, dict):
                if self.debug:
                    print(f"Warning: Expected dict for time, got {type(parsed)}")
                return None

            time_val = parsed.get("time")
            if not time_val:
                if self.debug:
                    print(f"Warning: Time missing value: {parsed}")
                return None

            calendarmodel = parsed.get("calendarmodel") or "http://www.wikidata.org/entity/Q1985786"
            cal_id = calendarmodel.rsplit("/", 1)[-1] if isinstance(calendarmodel, str) else "Q1985786"

            try:
                string_val = wikidata_time_to_text(
                    parsed,
                    self.lang,
                )
            except (ValueError, TypeError, KeyError, requests.RequestException) as e:
                if self.debug:
                    print(f"Warning: Failed to parse time value {time_val}: {e}")
                return None

            return WikidataTime(
                time=time_val,
                precision=parsed.get("precision"),
                calendarmodel=cal_id,
                string_val=string_val,
            )

        if datatype == "globe-coordinate":
            if not isinstance(parsed, dict):
                if self.debug:
                    print(f"Warning: Expected dict for coordinates, got {type(parsed)}")
                return None

            lat = parsed.get("latitude")
            lon = parsed.get("longitude")
            if lat is None or lon is None:
                if self.debug:
                    print(f"Warning: Coordinates missing lat/lon: {parsed}")
                return None

            try:
                string_val = wikidata_geolocation_to_text(parsed, self.lang)
            except (ValueError, TypeError, KeyError, requests.RequestException) as e:
                if self.debug:
                    print(f"Warning: Failed to parse coordinates ({lat}, {lon}): {e}")
                return None

            return WikidataCoordinates(latitude=lat, longitude=lon, string_val=string_val)

        # monolingualtext objects are represented as dicts in your parsing layer
        if isinstance(parsed, dict) and "text" in parsed:
            if parsed.get("language") != self.lang:
                return WikidataText(text=None)
            return WikidataText(text=parsed.get("text"))

        return WikidataText(text=str(parsed))

    # -------------------------------------------------------------------------
    # Main value extraction
    # -------------------------------------------------------------------------

    def _main_value(self, statement_node: URIRef, pid: str, datatype: str) -> Any:
        """Extract the main value for pid from a validated statement node."""
        if not self._is_statement_node(statement_node, pid):
            if self.debug:
                print(f"Warning: {pid} main_value called with non-statement node: {statement_node}")
            return None

        rich_node = self.g.value(statement_node, PSV[pid])
        if isinstance(rich_node, URIRef):
            parsed = self._parse_rich_value_node(datatype, rich_node)
            if parsed is not None:
                return parsed

        v = self.g.value(statement_node, PS[pid])
        if v is None:
            # If datatype was unknown (or wrong), attempt light inference from rich node
            if isinstance(rich_node, URIRef):
                inferred = self._infer_from_rich_node(rich_node)
                if inferred is not None:
                    return inferred

            if self.debug:
                print(f"Warning: No value found for {pid} (datatype={datatype}) in statement {statement_node}")
            return None

        return self._parse_ps_value(datatype, v)

    def _parse_rich_value_node(self, datatype: str, node: URIRef) -> Optional[Dict[str, Any]]:
        if datatype == "time":
            r = self._time_from_node(node)
            return r if r.get("time") else None
        if datatype == "quantity":
            r = self._quantity_from_node(node)
            return r if r.get("amount") is not None else None
        if datatype == "globe-coordinate":
            r = self._coord_from_node(node)
            return r if r.get("latitude") is not None and r.get("longitude") is not None else None
        return None

    def _infer_from_rich_node(self, node: URIRef) -> Optional[Dict[str, Any]]:
        # Lightweight inference, same effective behavior as before.
        if self.g.value(node, WIKIBASE.timeValue) is not None:
            r = self._time_from_node(node)
            return r if r.get("time") else None
        if self.g.value(node, WIKIBASE.quantityAmount) is not None:
            r = self._quantity_from_node(node)
            return r if r.get("amount") is not None else None
        if self.g.value(node, WIKIBASE.geoLatitude) is not None:
            r = self._coord_from_node(node)
            return r if r.get("latitude") is not None and r.get("longitude") is not None else None
        return None

    def _parse_ps_value(self, datatype: str, v: Any) -> Any:
        if datatype == "time":
            if isinstance(v, URIRef):
                r = self._time_from_node(v)
                return r if r.get("time") else None
            if isinstance(v, Literal):
                s = str(v)
                if not s:
                    return None
                # Preserve your behavior around time string normalization
                if s.endswith("Z") or ("+" in s[10:] or "-" in s[10:]):
                    return {"time": s, "precision": None, "calendarmodel": None}
                return {"time": s + "Z", "precision": None, "calendarmodel": None}

        if datatype == "wikibase-item" and isinstance(v, URIRef):
            return self._qid_from_wd_uri(v)

        if datatype == "monolingualtext" and isinstance(v, Literal):
            return {"text": str(v), "language": v.language or ""}

        if isinstance(v, Literal):
            return str(v)

        return str(v)

    # -------------------------------------------------------------------------
    # Qualifiers and references
    # -------------------------------------------------------------------------

    def _qualifiers(self, statement_node: URIRef) -> Dict[str, List[Dict[str, Any]]]:
        out: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)

        for pred, obj in self.g.predicate_objects(statement_node):
            if not isinstance(pred, URIRef):
                continue

            sp = str(pred)
            if not sp.startswith(str(PQ)):
                continue
            # Reject rich-value and normalized qualifier predicates
            if sp.startswith(str(PQV)) or sp.startswith(str(PQN)):
                continue

            qpid = self._pid_from_prop_uri(pred)
            if not qpid:
                continue

            datatype = self._prop_datatype(qpid)
            rich = self.g.value(statement_node, PQV[qpid])

            val = self._snak_value(datatype, obj, rich)
            if val is not None:
                out[qpid].append({"value": val})

        return dict(out)

    def _references(self, statement_node: URIRef) -> List[Dict[str, Any]]:
        refs: List[Dict[str, Any]] = []

        for ref_node in self.g.objects(statement_node, PROV.wasDerivedFrom):
            if not isinstance(ref_node, URIRef):
                continue

            snaks: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)

            for pred, obj in self.g.predicate_objects(ref_node):
                if not isinstance(pred, URIRef):
                    continue

                sp = str(pred)
                if not sp.startswith(str(PR)):
                    continue
                # Reject rich-value and normalized reference predicates
                if sp.startswith(str(PRV)) or sp.startswith(str(PRN)):
                    continue

                rpid = self._pid_from_prop_uri(pred)
                if not rpid:
                    continue

                datatype = self._prop_datatype(rpid)
                rich = self.g.value(ref_node, PRV[rpid])

                val = self._snak_value(datatype, obj, rich)
                if val is not None:
                    snaks[rpid].append({"value": val})

            refs.append({"snaks": dict(snaks)})

        return refs

    def _snak_value(self, datatype: str, obj: Any, rich_node: Any) -> Any:
        # external-id: keep literal, ignore normalized URI forms
        if datatype == "external-id":
            return str(obj) if isinstance(obj, Literal) else None

        if datatype == "time":
            for candidate in (rich_node, obj):
                if isinstance(candidate, URIRef):
                    r = self._time_from_node(candidate)
                    if r.get("time"):
                        return r
            if isinstance(obj, Literal):
                s = str(obj)
                return {"time": s, "precision": None, "calendarmodel": None} if s else None
            return None

        if datatype == "quantity":
            if isinstance(rich_node, URIRef):
                r = self._quantity_from_node(rich_node)
                if r.get("amount") is not None:
                    return r
            if isinstance(obj, Literal):
                return {"amount": str(obj), "unit": "1"}
            return None

        if datatype == "globe-coordinate":
            if isinstance(rich_node, URIRef):
                r = self._coord_from_node(rich_node)
                return r if r.get("latitude") is not None and r.get("longitude") is not None else None
            return None

        if datatype == "wikibase-item" and isinstance(obj, URIRef):
            return self._qid_from_wd_uri(obj)

        if datatype == "monolingualtext" and isinstance(obj, Literal):
            return {"text": str(obj), "language": obj.language or ""}

        if isinstance(obj, Literal):
            return str(obj)

        return str(obj)

    # -------------------------------------------------------------------------
    # Rich node parsing helpers
    # -------------------------------------------------------------------------

    def _quantity_from_node(self, node: URIRef) -> Dict[str, Any]:
        amount = self.g.value(node, WIKIBASE.quantityAmount)
        unit = self.g.value(node, WIKIBASE.quantityUnit)
        upper = self.g.value(node, WIKIBASE.quantityUpperBound)
        lower = self.g.value(node, WIKIBASE.quantityLowerBound)

        out: Dict[str, Any] = {
            "amount": str(amount) if amount is not None else None,
            "unit": str(unit) if unit is not None else "1",
        }
        if upper is not None:
            out["upperBound"] = str(upper)
        if lower is not None:
            out["lowerBound"] = str(lower)
        return out

    def _time_from_node(self, node: URIRef) -> Dict[str, Any]:
        time_v = self.g.value(node, WIKIBASE.timeValue)
        precision = self.g.value(node, WIKIBASE.timePrecision)
        calendar = self.g.value(node, WIKIBASE.timeCalendarModel)

        return {
            "time": str(time_v) if time_v is not None else None,
            "precision": int(precision) if isinstance(precision, Literal) and precision.value is not None else None,
            "calendarmodel": str(calendar) if calendar is not None else None,
        }

    def _coord_from_node(self, node: URIRef) -> Dict[str, Any]:
        lat = self.g.value(node, WIKIBASE.geoLatitude)
        lon = self.g.value(node, WIKIBASE.geoLongitude)
        globe = self.g.value(node, WIKIBASE.geoGlobe)
        prec = self.g.value(node, WIKIBASE.geoPrecision)

        return {
            "latitude": float(lat) if isinstance(lat, Literal) and lat.value is not None else None,
            "longitude": float(lon) if isinstance(lon, Literal) and lon.value is not None else None,
            "globe": str(globe) if globe is not None else None,
            "precision": float(prec) if isinstance(prec, Literal) and prec.value is not None else None,
        }

    # -------------------------------------------------------------------------
    # TTL label helpers
    # -------------------------------------------------------------------------

    def _lang_value_map(self, subj: URIRef, pred: URIRef) -> Dict[str, Dict[str, str]]:
        out: Dict[str, Dict[str, str]] = {}
        for o in self.g.objects(subj, pred):
            if isinstance(o, Literal) and o.language:
                out[o.language] = {"language": o.language, "value": str(o)}
        return out

    def _aliases_lang_map(self, subj: URIRef, pred: URIRef) -> Dict[str, List[Dict[str, str]]]:
        out: DefaultDict[str, List[Dict[str, str]]] = defaultdict(list)
        for o in self.g.objects(subj, pred):
            if isinstance(o, Literal) and o.language:
                out[o.language].append({"language": o.language, "value": str(o)})
        return dict(out)

    # -------------------------------------------------------------------------
    # Datatype and ID helpers
    # -------------------------------------------------------------------------

    def _prop_datatype(self, pid: str, statement_node: Optional[URIRef] = None) -> str:
        """Return property datatype name. Defaults to 'string'."""
        prop_uri = WD[pid]
        ptype = self.g.value(prop_uri, WIKIBASE.propertyType)

        if isinstance(ptype, URIRef):
            s = str(ptype)
            if s.endswith("#WikibaseItem"):
                return "wikibase-item"
            if s.endswith("#Quantity"):
                return "quantity"
            if s.endswith("#Time"):
                return "time"
            if s.endswith("#GlobeCoordinate"):
                return "globe-coordinate"
            if s.endswith("#ExternalId"):
                return "external-id"
            if s.endswith("#Monolingualtext"):
                return "monolingualtext"
            if s.endswith("#Url"):
                return "url"
            if s.endswith("#String"):
                return "string"
            if s.endswith("#CommonsMedia"):
                return "commonsMedia"
            if s.endswith("#GeoShape"):
                return "geoShape"
            if s.endswith("#TabularData"):
                return "tabular-data"
            if s.endswith("#Math"):
                return "math"
            if s.endswith("#MusicalNotation"):
                return "musical-notation"

        # Inference from an example statement node, if provided
        if statement_node is not None:
            rich = self.g.value(statement_node, PSV[pid])
            if isinstance(rich, URIRef):
                if self.g.value(rich, WIKIBASE.timeValue) is not None:
                    return "time"
                if self.g.value(rich, WIKIBASE.quantityAmount) is not None:
                    return "quantity"
                if self.g.value(rich, WIKIBASE.geoLatitude) is not None:
                    return "globe-coordinate"

            ps_v = self.g.value(statement_node, PS[pid])
            if isinstance(ps_v, URIRef):
                qid = self._qid_from_wd_uri(ps_v)
                if qid and qid.startswith("Q"):
                    return "wikibase-item"
            if isinstance(ps_v, Literal) and ps_v.datatype:
                dt = str(ps_v.datatype)
                if dt.endswith("dateTime"):
                    return "time"
                if dt.endswith("decimal"):
                    return "quantity"

        if self.debug:
            print(f"Warning: Property {pid} definition not found in TTL; defaulting to 'string'")
        return "string"

    @staticmethod
    def _qid_from_wd_uri(u: Any) -> Optional[str]:
        if not isinstance(u, URIRef):
            return None
        s = str(u)
        if not s.startswith(str(WD)):
            return None
        tail = s.rsplit("/", 1)[-1]
        if (tail.startswith("Q") or tail.startswith("P")) and tail[1:].isdigit():
            return tail
        return None

    @staticmethod
    def _pid_from_prop_uri(u: Any) -> Optional[str]:
        if not isinstance(u, URIRef):
            return None
        s = str(u)
        if not s.startswith((str(P), str(PS), str(PQ), str(PR))):
            return None
        tail = s.rsplit("/", 1)[-1]
        if tail.startswith("P") and tail[1:].isdigit():
            return tail
        return None

    @staticmethod
    def _rank_to_str(rank_uri: URIRef) -> Optional[str]:
        s = str(rank_uri)
        if s.endswith("PreferredRank"):
            return "preferred"
        if s.endswith("NormalRank"):
            return "normal"
        if s.endswith("DeprecatedRank"):
            return "deprecated"
        return None
