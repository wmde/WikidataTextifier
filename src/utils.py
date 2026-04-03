"""HTTP helpers and value-formatting utilities for Wikidata APIs."""

import html
import json
import os

import requests
from requests.adapters import HTTPAdapter

REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "15"))

SESSION = requests.Session()
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)


def get_wikidata_ttl_by_id(
    id,
    lang="en",
):
    """Fetch a Wikidata entity as TTL from ``Special:EntityData``.

    Args:
        id (str): Wikidata entity ID, for example ``"Q42"`` or ``"P31"``.
        lang (str, optional): Language code for server-side label rendering.

    Returns:
        str: TTL document for the requested entity.

    Raises:
        requests.HTTPError: If Wikidata returns an error response.
    """
    params = {
        "uselang": lang,
    }
    headers = {"User-Agent": "Wikidata Textifier (embeddings@wikimedia.de)"}

    response = SESSION.get(
        f"https://www.wikidata.org/wiki/Special:EntityData/{id}.ttl",
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.text


def get_wikidata_json_by_ids(ids, props="labels|descriptions|aliases|claims"):
    """Fetch one or more Wikidata entities from ``wbgetentities``.

    Args:
        ids (list[str] | str): Entity IDs as a list or ``|``-separated string.
        props (str): Pipe-delimited properties requested from the API.

    Returns:
        dict[str, dict]: Mapping of entity IDs to API entity payloads.

    Raises:
        requests.HTTPError: If Wikidata returns an error response.
    """
    if isinstance(ids, str):
        ids = ids.split("|")
    ids = list(dict.fromkeys(ids))  # Ensure unique IDs

    entities_data = {}

    # Wikidata API has a limit on the number of IDs per request,
    # typically 50 for wbgetentities.
    for chunk_idx in range(0, len(ids), 50):
        ids_chunk = ids[chunk_idx : chunk_idx + 50]
        params = {
            "action": "wbgetentities",
            "ids": "|".join(ids_chunk),
            "props": props,
            "format": "json",
            "origin": "*",
        }
        headers = {"User-Agent": "Wikidata Textifier (embeddings@wikimedia.de)"}

        response = SESSION.get(
            "https://www.wikidata.org/w/api.php?",
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        chunk_data = response.json().get("entities", {})
        entities_data = entities_data | chunk_data

    return entities_data


#####################################
# Formatting
#####################################


def wikidata_time_to_text(value: dict, lang: str = "en"):
    """Format a time datavalue into localized display text using a local Wikibase instance.

    Args:
        value (dict): Time value payload in Wikibase datavalue format.
        lang (str): Language code used by ``wbformatvalue``.

    Returns:
        str: Localized human-readable representation of the time value.

    Raises:
        ValueError: If the input payload is invalid or the API response is malformed.
        requests.HTTPError: If the formatting API returns an error response.
    """
    WIKIBASE_HOST = os.environ.get("WIKIBASE_HOST", "wikibase")
    WIKIBASE_API = f"http://{WIKIBASE_HOST}/w/api.php"

    time = value.get("time")
    if not isinstance(time, str) or not time:
        raise ValueError("Invalid or missing time value")
    if time.endswith("+00:00"):
        time = time[:-6] + "Z"
    if not time.startswith("+") and not time.startswith("-"):
        time = "+" + time

    datavalue = {
        "type": "time",
        "value": {
            "time": time,
            "timezone": value.get("timezone", 0),
            "before": value.get("before", 0),
            "after": value.get("after", 0),
            "precision": value.get("precision", 10),
            "calendarmodel": value.get("calendarmodel", "Q1985786"),
        },
    }

    r = SESSION.post(
        WIKIBASE_API,
        data={
            "action": "wbformatvalue",
            "format": "json",
            "uselang": lang,
            "datavalue": json.dumps(datavalue),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    r.raise_for_status()

    data = r.json()
    if "result" not in data:
        raise ValueError("Missing 'result' in wbformatvalue response")
    return html.unescape(data["result"])


def wikidata_geolocation_to_text(value: dict, lang: str = "en"):
    """Format a globe-coordinate value into localized display text using a local Wikibase instance.

    Args:
        value (dict): Coordinate payload in Wikibase datavalue format.
        lang (str): Language code used by ``wbformatvalue``.

    Returns:
        str: Localized human-readable representation of the coordinate value.

    Raises:
        ValueError: If the formatting API response is malformed.
        requests.HTTPError: If the formatting API returns an error response.
    """
    WIKIBASE_HOST = os.environ.get("WIKIBASE_HOST", "wikibase")
    WIKIBASE_API = f"http://{WIKIBASE_HOST}/w/api.php"

    datavalue = {
        "type": "globecoordinate",
        "value": {
            "latitude": value.get("latitude"),
            "longitude": value.get("longitude"),
            "altitude": value.get("altitude", None),
            "precision": value.get("precision", 0),
            "globe": value.get("globe", "http://www.wikidata.org/entity/Q2"),
        },
    }

    r = SESSION.post(
        WIKIBASE_API,
        data={
            "action": "wbformatvalue",
            "format": "json",
            "uselang": lang,
            "datavalue": json.dumps(datavalue),
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    r.raise_for_status()

    data = r.json()
    if "result" not in data:
        raise ValueError("Missing 'result' in wbformatvalue response")
    return html.unescape(data["result"])
