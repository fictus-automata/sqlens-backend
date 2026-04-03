# SQLens: SQL Query Service with Lineage (v0 MVP)

## Interpretation & assumptions

The original requirement is intentionally open-ended. For the MVP (“v0”) I assumed:

- Clients submit SQL text and a `user_id`; there is no authentication/authorization layer in MVP.
- The system stores each submitted query with server-generated `created_at`.
- “Lineage” in v0 is a best-effort extraction of:
  - `tables`: extracted from `FROM` and `JOIN` clauses
  - `columns`: extracted from SQLGlot’s AST (unqualified column names, includes column references inside expressions like `JOIN ... ON`)
- Lineage extraction uses `sqlglot` (AST-based) rather than token heuristics.
- The AI-driven query improvement endpoint is **omitted** for MVP, per your preference; the codebase documents how it would be added later.

## API (MVP)

Base server: `http://localhost:8000`

`POST /queries`

- Request body:
  - `user_id`: string
  - `query_text`: string
  - `query_name`: string (optional)
  - `tags`: JSON object (optional)
- Response: `{ "id": UUID, "user_id": string, "created_at": datetime, "lineage_status": "completed"|"failed" }`

`GET /queries`

- Query params:
  - `user_id` (optional)
  - `created_after` (optional, ISO-8601 datetime)
  - `table_name` (optional)
  - `tags` (optional JSON string, e.g. `?tags={"env":"prod"}`)
  - `limit` (default `50`)
  - `offset` (default `0`)
- Response:
  - `{ "items": [{ "id", "user_id", "query_text", "query_name", "tags", "created_at", "lineage_status" }...], "limit", "offset" }`

`GET /queries/{id}`

- Response: `{ "id", "user_id", "query_text", "query_name", "tags", "created_at", "updated_at", "lineage_status", "parse_error" }`

`GET /queries/{id}/lineage`

- Response: `{ "tables": [..], "columns": [..], "column_lineage": { "<column_name>": ["<table1>", "<table2>"] } }`
- Behavior:
  - Lineage is computed and persisted during `POST /queries`.
  - If lineage extraction failed, the endpoint returns `tables=[]`, `columns=[]`, and `column_lineage={}`.

## Incremental evolution (v0 -> v1 -> v2)

### v0 (this MVP)

- FastAPI backend
- Postgres persistence (default, async SQLAlchemy), with SQLite support used in tests
- `sqlglot` AST-based lineage extraction
- Lineage persisted at ingest time into `query_lineage`
- `lineage_status` + `parse_error` persisted on the query row

### v1 (next step)

Normalize output further while keeping the API stable:

- Decide on whether `columns` should be qualified (`alias.col`) or normalized/unqualified (`col`).
- Add explicit handling for derived tables/CTEs (naming + attribution).
- Expand dialect/format coverage and add more regression tests for real-world SQL.

### v2 (future)

Add the AI-driven query enhancement endpoint:

- New endpoint: `POST /queries/{id}/suggestions`
- Inputs: stored query (and optional “intent” like performance vs readability)
- Outputs: improvement suggestions + safety metadata
- Production concerns:
  - rate limiting
  - timeouts/circuit breakers
  - observability around LLM failures

Per your preference, v0 does **not** expose the AI endpoint.

## Architectural decisions & trade-offs

- Framework: **FastAPI**
  - Straightforward request validation via Pydantic
  - Clean dependency injection for DB sessions
- ORM: **SQLAlchemy 2.x**
  - Clear model definitions and portable querying
- Data model (`sql_queries`)
  - Stores the query (`query_text`) + metadata (`user_id`, `query_name`, `tags`)
  - Uses UUID primary keys and server-managed timestamps (`created_at`, `updated_at`)
- Lineage model (`query_lineage`)
  - Persisted lineage rows so lineage can be queried independently of the API read path
- Lineage in v0 is intentionally limited
  - Trade-off: fast to implement vs perfect correctness
  - v1 upgrades parsing fidelity while keeping the same storage/API contracts

## Performance & scalability plans

Starting point:

- DB indexes:
  - `(user_id, created_at)` supports filter + pagination patterns
- Lineage persistence:
  - lineage rows are written once during `POST /queries` and then read efficiently from `query_lineage`

Scaling to large volumes (10M queries/day, high concurrency):

- Horizontal scaling of the API:
  - stateless FastAPI instances behind a load balancer
- Postgres:
  - ensure proper indexing (and consider covering indexes for query patterns)
  - partition `sql_queries` by time (e.g., monthly) for pruning
  - consider read replicas for heavy `GET /queries` traffic
- Asynchronous ingestion (v1):
  - move lineage extraction into a background worker so `POST /queries` stays fast
- Read scaling:
  - add read replicas for `GET /queries` and `GET /queries/{id}/lineage`

## Debugging & observability approach

What’s included in MVP:

- Request correlation: `X-Request-ID` middleware (see `app/main.py`)
- Structured log setup (`app/core/logging.py`)

What I would add next for production:

- Metrics:
  - request counts, latency histograms per endpoint
- Tracing:
  - OpenTelemetry tracing across request lifecycle (API -> DB -> background jobs)
- Better error taxonomy:
  - map parse errors vs transient DB issues into consistent error codes

## AI integration strategy (MVP omitted)

Since the AI endpoint is omitted in v0:

- The codebase is structured so AI can be added later without changing v0 lineage storage.
- When added (v2):
  - guardrails for unpredictable LLM responses
  - strict timeouts, retries with backoff, and rate limiting
  - store AI outputs (and model/version metadata) for auditability

## Use of AI tools

During development I used AI assistance to:

- Translate the ambiguous problem statement into concrete MVP decisions
  - v0 lineage heuristics (what to extract, error conditions)
  - API contracts and DB schema
- Implement and validate the `sqlglot`-based lineage extraction logic
- Scaffold integration tests (FastAPI + SQLAlchemy) and adjust for serialization/timestamp nuances

Specifically, the implementation and test structure in this repo were guided by iterative AI-assisted refactoring of API shape, ORM model typing, and lineage parsing heuristics.

## How to run

### 1) Create venv + install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure database

SQLite (recommended for local setup / no external dependencies):

```bash
export DATABASE_URL="sqlite+aiosqlite:///./altimate.db"
```

Postgres (example):

```bash
export DATABASE_URL="postgresql+psycopg://postgres:postgres@localhost:5432/altimate"
```

### 3) Initialize tables (dev-only)

```bashpython -c "import asyncio; from app.db.session import init_db; asyncio.run(init_db())"
.venv/bin/
```

Note: this creates tables directly from ORM metadata (no Alembic migrations). For Postgres, ensure the configured role/database exist and Postgres is running.

### 4) Start server

```bash
uvicorn app.main:app --reload
```

Server health endpoint:

- `GET /healthz`

### 5) Run tests

```bash
.venv/bin/pytest -q
```
