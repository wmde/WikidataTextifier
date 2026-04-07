# Tests

This folder contains automated tests split into two layers:

- **Unit (`tests/unit`)**: Fast isolated tests with stubs/mocks and direct function/route calls.
- **Integration (`tests/integration`)**: Live HTTP tests against a running local API server.

## What Is Covered

### Unit (`tests/unit`)

- Route wiring behavior for single-ID vs multi-ID normalization paths.
- JSON normalizer behavior (rank filtering, datatype conversion, external-id filtering).
- Textifier model behavior (serialization, triplet/text rendering, truthiness rules).
- Utility helpers (`src/utils.py`) with mocked HTTP calls.
- Label helper behavior (`src/WikidataLabel.py`) including language fallback and lazy resolution.

### Integration (`tests/integration`)

- Local API contracts for `GET /` and docs endpoint availability.
- Response shape checks for JSON and text output.
- Cache verification: ensure label rows are written and reused between repeated requests.

## Setup

From project root:

```bash
uv sync --locked
```

For integration tests, start Docker services first:

```bash
docker compose up --build
```

## Common Commands

Run unit tests only:

```bash
uv run pytest -q tests/unit
```

Run integration tests only:

```bash
uv run pytest -q tests/integration -m integration
```

Run all tests:

```bash
uv run pytest -q tests
```

Run lint:

```bash
uv run ruff check .
```

## Notes

- Integration tests assume the API is available at `http://127.0.0.1:5000`.
- The cache integration test reads DB credentials from environment variables or local `.env`.
- If DB credentials are not usable, the cache verification test is skipped with a clear message.
