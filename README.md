# Wikidata Textifier

**Wikidata Textifier** is an API that transforms Wikidata entities into compact outputs for LLM and GenAI use cases.
It resolves missing labels for properties and claim values using the Wikidata Action API and caches labels to reduce repeated lookups.

Live API: [wd-textify.wmcloud.org](https://wd-textify.wmcloud.org/) \
API Docs: [wd-textify.wmcloud.org/docs](https://wd-textify.wmcloud.org/docs)

## Features

- Textify Wikidata entities as `json`, `text`, or `triplet`.
- Resolve labels for linked entities and properties.
- Cache labels in MariaDB for faster repeated requests.
- Support multilingual output with fallback language support.
- Avoid SPARQL and use Wikidata Action API / EntityData endpoints.

## Output Formats

- `json`: Structured representation with claims (and optionally qualifiers/references).
- `text`: Readable summary including label, description, aliases, and attributes.
- `triplet`: Triplet-style lines with labels and IDs for graph-style traversal.

## API

### `GET /`

#### Query parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `id` | string | Yes | Comma-separated Wikidata IDs (for example: `Q42` or `Q42,Q2`). |
| `pid` | string | No | Comma-separated property IDs to filter claims (for example: `P31,P279`). |
| `lang` | string | No | Preferred language code (default: `en`). |
| `fallback_lang` | string | No | Fallback language code (default: `en`). |
| `format` | string | No | Output format: `json`, `text`, or `triplet` (default: `json`). |
| `external_ids` | bool | No | Include `external-id` datatype claims (default: `true`). |
| `all_ranks` | bool | No | Include all statement ranks instead of preferred/normal filtering (default: `false`). |
| `qualifiers` | bool | No | Include qualifiers in claim values (default: `true`). |
| `references` | bool | No | Include references in claim values (default: `false`). |

#### Example requests

```bash
curl "https://wd-textify.wmcloud.org/?id=Q42"
curl "https://wd-textify.wmcloud.org/?id=Q42&format=text&lang=en"
curl "https://wd-textify.wmcloud.org/?id=Q42,Q2&pid=P31,P279&format=triplet"
```
