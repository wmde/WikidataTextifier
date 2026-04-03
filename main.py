"""FastAPI application that exposes Wikidata textification endpoints."""

import os
import time
import traceback

import requests
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from src import utils
from src.Normalizer import JSONNormalizer, TTLNormalizer
from src.WikidataLabel import LazyLabelFactory, WikidataLabel

# Start Fastapi app
app = FastAPI(
    title="Wikidata Textifier",
    description="Transforms Wikidata entities into text representations.",
    version="1.0.0",
    docs_url="/docs",  # Change the Swagger UI path if needed
    redoc_url="/redoc",  # Change the ReDoc path if needed
    swagger_ui_parameters={"persistAuthorization": True},
)

# Enable all Cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

LABEL_CLEANUP_INTERVAL_SECONDS = int(os.environ.get("LABEL_CLEANUP_INTERVAL_SECONDS", 3600))
_last_label_cleanup = 0.0

@app.on_event("startup")
async def startup():
    """Initialize database resources required by the API."""
    WikidataLabel.initialize_database()

@app.get(
    "/",
    responses={
        200: {
            "description": "Returns a list of relevant Wikidata property PIDs with similarity scores",
            "content": {
                "application/json": {
                    "example": [{
                        "Q42": "Douglas Adams (human), English writer, humorist, and dramatist...",
                    }]
                }
            },
        },
        422: {
            "description": "Missing or invalid query parameter",
            "content": {
                "application/json": {
                    "example": {"detail": "Invalid format specified"}
                }
            },
        },
    },
)
async def get_textified_wd(
    request: Request, background_tasks: BackgroundTasks,
    id: str = Query(..., examples="Q42,Q2"),
    pid: str = Query(None, examples="P31,P279"),
    lang: str = 'en',
    format: str = 'json',
    external_ids: bool = True,
    references: bool = False,
    all_ranks: bool = False,
    qualifiers: bool = True,
    fallback_lang: str = 'en'
):
    """Retrieve Wikidata entities as structured JSON, natural text, or triplet lines.

    This endpoint fetches one or more entities, resolves missing labels, and normalizes
    claims into a compact representation suitable for downstream LLM use.

    **Args:**

    - **id** (str): Comma-separated Wikidata IDs to fetch (for example: `"Q42"` or `"Q42,Q2"`).
    - **pid** (str, optional): Comma-separated property IDs used to filter returned claims (for example: `"P31,P279"`).
    - **lang** (str): Preferred language code for labels and formatted values.
    - **format** (str): Output format. One of `"json"`, `"text"`, or `"triplet"`.
    - **external_ids** (bool): If `true`, include claims with datatype `external-id`.
    - **references** (bool): If `true`, include references in claim values (JSON output only).
    - **all_ranks** (bool): If `true`, include preferred, normal, and deprecated statement ranks.
    - **qualifiers** (bool): If `true`, include qualifiers for claim values.
    - **fallback_lang** (str): Fallback language used when `lang` is unavailable.
    - **request** (Request): FastAPI request context object.
    - **background_tasks** (BackgroundTasks): Background task manager used for cache cleanup.

    **Returns:**

    A dictionary keyed by requested entity ID (for example, `"Q42"`).  
    Each value depends on `format`:

    - **json**: Structured entity payload with label, description, aliases, and claims.
    - **text**: Human-readable summary text.
    - **triplet**: Triplet-style text lines with labels and IDs.
    """
    try:
        filter_pids = []
        if pid:
            filter_pids = [p.strip() for p in pid.split(',')]

        qids = [q.strip() for q in id.split(',')]
        label_factory = LazyLabelFactory(lang=lang, fallback_lang=fallback_lang)

        entities = {}
        if len(qids) == 1:
            # When one QID is requested, TTL is used
            try:
                entity_data = utils.get_wikidata_ttl_by_id(qids[0], lang=lang)
            except requests.HTTPError:
                entity_data = None

            if not entity_data:
                response = "ID not found"
                raise HTTPException(status_code=404, detail=response)

            entity_data = TTLNormalizer(
                entity_id=qids[0],
                ttl_text=entity_data,
                lang=lang,
                fallback_lang=fallback_lang,
                label_factory=label_factory,
                debug=False,
            )

            entities = {
                qids[0]: entity_data.normalize(
                    external_ids=external_ids,
                    all_ranks=all_ranks,
                    references=references,
                    filter_pids=filter_pids,
                    qualifiers=qualifiers,
                )
            }
        else:
            # JSON is used with Action API for bulk retrieval
            try:
                entity_data = utils.get_wikidata_json_by_ids(qids)
            except requests.HTTPError:
                entity_data = None
            if not entity_data:
                response = "IDs not found"
                raise HTTPException(status_code=404, detail=response)

            entity_data = {
                qid: JSONNormalizer(
                    entity_id=qid,
                    entity_json=entity_data[qid],
                    lang=lang,
                    fallback_lang=fallback_lang,
                    label_factory=label_factory,
                    debug=False,
                ) if entity_data.get(qid) else None
                for qid in qids
            }

            entities = {
                qid: entity.normalize(
                    external_ids=external_ids,
                    all_ranks=all_ranks,
                    references=references,
                    filter_pids=filter_pids,
                    qualifiers=qualifiers
                ) if entity else None
                for qid, entity in entity_data.items()
            }

        return_data = {}
        for qid, entity in entities.items():
            if not entity:
                return_data[qid] = None
                continue

            if format == 'text':
                results = entity.to_text(lang)
            elif format == 'triplet':
                results = entity.to_triplet()
            else:
                results = entity.to_json()

            return_data[qid] = results

        global _last_label_cleanup
        if time.time() - _last_label_cleanup > LABEL_CLEANUP_INTERVAL_SECONDS:
            background_tasks.add_task(WikidataLabel.delete_old_labels)
            _last_label_cleanup = time.time()

        return return_data

    except HTTPException:
        raise
    except requests.RequestException:
        raise HTTPException(status_code=502, detail="Upstream service unavailable")
    except Exception:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")
