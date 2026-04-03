# Design Journal

This journal records the key design decisions and how the solution evolved from the initial “project basis” to a working MVP.

## 1. Interpreting the open-ended requirement
The requirement describes three capabilities (query storage/retrieval, lineage extraction, and an AI enhancement stretch goal) but leaves many details ambiguous.

I translated that into an MVP approach:
- Provide a minimal CRUD-like HTTP API for storing and listing SQL queries.
- Persist lineage at ingest time and expose endpoints to fetch it later.
- Defer the AI endpoint entirely (per your preference) while documenting how it would be integrated later.

## 2. Choosing the v0 tech stack
I selected:
- FastAPI for the backend API layer
- SQLAlchemy (2.x) for the ORM
- Postgres as the default storage backend
- `sqlglot` as the v0 lineage extractor (AST-based parsing)

The repository started with skeleton modules to ensure the service could boot quickly, even before the full DB and endpoint logic was implemented.

## 3. DB schema and session wiring (v0 persistence)
For v0 persistence, I implemented the plan-aligned schema:
- `sql_queries`
  - UUID primary key (`id`)
  - `user_id`
  - `query_text` + optional `query_name`
  - flexible `tags` stored as JSON/JSONB
  - `created_at` and `updated_at`
  - `lineage_status` + `parse_error` for ingest-time extraction failures
- `query_lineage`
  - UUID primary key
  - stores extracted `(table_name, column_name)` pairs with a `lineage_type`

This design supports:
- Fast retrieval by `user_id` and time windows
- Storing lineage once per query, avoiding repeated parsing work

For development/testing, I added an async `init_db()` helper that creates tables directly from ORM metadata (skipping Alembic migrations for now).

## 4. Implementing the API endpoints
I implemented the plan’s MVP API surface:
- `POST /queries` stores the query and runs lineage extraction inline (sync for v0)
- `GET /queries` supports filtering (`user_id`, `created_after`, `table_name`, `tags`) + pagination
- `GET /queries/{id}` returns the stored query plus `lineage_status`/`parse_error`
- `GET /queries/{id}/lineage` returns persisted lineage (empty lists on ingest-time failure)

Error handling was designed to keep failures actionable:
- 404 for unknown query IDs
- Ingest never fails: `POST /queries` always returns `201`, and parse failures are recorded via `lineage_status=failed` + `parse_error`. `GET /queries/{id}/lineage` returns empty lists.

## 5. Lineage extraction (v0)
For v0 lineage extraction, I used deterministic SQLGlot AST traversal:
- Tables: extracted from `FROM` / `JOIN` table references (`exp.Table`)
- Columns: extracted from all column references found in the query tree (`exp.Column`), including columns used in `JOIN ... ON`

I also kept an in-process `lru_cache` on `compute_lineage(sql)` to reduce repeated parsing during testing/dev.
- Ensures deterministic results within a running process
- Reduces CPU usage for repeated calls with the same SQL text

## 6. Tests and iteration
I added:
- Unit tests validating lineage extraction behavior (SELECT/FROM, JOIN + qualified columns, wildcard `*`, and invalid SQL rejection)
- Integration tests for API behavior using a temporary SQLite database

During test iteration, I adjusted the `created_after` filter test to account for JSON timestamp serialization differences (sub-second rounding).

## 7. Final MVP status
The repo now includes:
- A working FastAPI service
- SQLAlchemy async persistence + persisted lineage rows
- v0 lineage extraction with `sqlglot`
- Passing tests (`pytest`)
- Documentation describing assumptions, incremental evolution, trade-offs, scalability plans, observability, and explicit AI-tool usage

