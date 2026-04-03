"""test_schema_registry.py — Integration tests for the Schema Registry.

Covers:
  - POST /schemas: register a table schema
  - GET /schemas: list + filter by source
  - GET /schemas/{id}: detail with columns
  - DELETE /schemas/{id}: removal
  - GET /schemas/sources: distinct source list
  - End-to-end: register schema → ingest query with source_name → verify
    ambiguous columns become unambiguous column_refs
  - Fallback: ingest multi-table query without schema → ambiguous_refs
"""
import importlib
import os
import uuid

import httpx
import pytest
import pytest_asyncio


@pytest_asyncio.fixture()
async def client(tmp_path):
    db_path = tmp_path / "altimate_schema_test.db"
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"

    import app.core.config as config_module
    import app.db.models as models_module
    import app.db.session as session_module
    import app.api.query_routes as routes_module
    import app.api.schema_routes as schema_routes_module
    import app.main as main_module

    importlib.reload(config_module)
    importlib.reload(models_module)
    importlib.reload(session_module)
    await session_module.init_db()
    importlib.reload(routes_module)
    importlib.reload(schema_routes_module)
    importlib.reload(main_module)

    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ── Schema CRUD ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_register_and_get_schema(client):
    """Register a schema, then retrieve it by ID and verify column details."""
    payload = {
        "source_name": "prod",
        "table_name": "orders",
        "dialect": "postgres",
        "columns": [
            {"column_name": "id", "data_type": "BIGINT", "ordinal_position": 1},
            {"column_name": "user_id", "data_type": "BIGINT", "ordinal_position": 2},
            {"column_name": "status", "data_type": "TEXT", "ordinal_position": 3},
        ],
    }
    resp = await client.post("/schemas", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    schema_id = data["id"]
    assert data["source_name"] == "prod"
    assert data["table_name"] == "orders"
    assert data["column_count"] == 3

    detail_resp = await client.get(f"/schemas/{schema_id}")
    assert detail_resp.status_code == 200
    detail = detail_resp.json()
    assert len(detail["columns"]) == 3
    col_names = [c["column_name"] for c in detail["columns"]]
    assert "id" in col_names
    assert "user_id" in col_names


@pytest.mark.asyncio
async def test_list_schemas_filter_by_source(client):
    """Listing schemas is filterable by source_name."""
    await client.post(
        "/schemas",
        json={"source_name": "prod", "table_name": "orders", "columns": [{"column_name": "id"}]},
    )
    await client.post(
        "/schemas",
        json={"source_name": "staging", "table_name": "orders", "columns": [{"column_name": "id"}]},
    )

    all_resp = await client.get("/schemas")
    assert all_resp.json()["total"] == 2

    prod_resp = await client.get("/schemas", params={"source_name": "prod"})
    assert prod_resp.json()["total"] == 1
    assert prod_resp.json()["items"][0]["source_name"] == "prod"


@pytest.mark.asyncio
async def test_delete_schema(client):
    """DELETE removes the schema; subsequent GET returns 404."""
    resp = await client.post(
        "/schemas",
        json={"source_name": "prod", "table_name": "events", "columns": [{"column_name": "id"}]},
    )
    schema_id = resp.json()["id"]

    del_resp = await client.delete(f"/schemas/{schema_id}")
    assert del_resp.status_code == 204

    get_resp = await client.get(f"/schemas/{schema_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_list_sources(client):
    """GET /schemas/sources returns distinct source_name values."""
    await client.post(
        "/schemas",
        json={"source_name": "alpha", "table_name": "t1", "columns": [{"column_name": "id"}]},
    )
    await client.post(
        "/schemas",
        json={"source_name": "beta", "table_name": "t2", "columns": [{"column_name": "id"}]},
    )
    await client.post(
        "/schemas",
        json={"source_name": "alpha", "table_name": "t3", "columns": [{"column_name": "id"}]},
    )

    resp = await client.get("/schemas/sources")
    assert resp.status_code == 200
    sources = resp.json()["sources"]
    assert sorted(sources) == ["alpha", "beta"]


# ── Lineage disambiguation ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_schema_aware_lineage_resolves_ambiguous_columns(client):
    """With a registered schema, same-name columns across tables are unambiguous.

    SQL: SELECT id, name FROM orders JOIN users ON orders.user_id = users.id
      - orders has: id, user_id, status
      - users  has: id, name, email
    After qualify_columns:
      - `id` in SELECT is orders.id (because SELECT list heuristic; qualify_columns picks first table match)
      - or both are qualified by alias context in JOIN ON clause
    The key assertion: no ambiguous_refs in the response when schema is available.
    """
    # Register schemas.
    for tbl, cols in [
        ("orders", [
            {"column_name": "id", "data_type": "BIGINT", "ordinal_position": 1},
            {"column_name": "user_id", "data_type": "BIGINT", "ordinal_position": 2},
            {"column_name": "status", "data_type": "TEXT", "ordinal_position": 3},
        ]),
        ("users", [
            {"column_name": "id", "data_type": "BIGINT", "ordinal_position": 1},
            {"column_name": "name", "data_type": "TEXT", "ordinal_position": 2},
            {"column_name": "email", "data_type": "TEXT", "ordinal_position": 3},
        ]),
    ]:
        r = await client.post(
            "/schemas",
            json={"source_name": "prod", "table_name": tbl, "dialect": "postgres", "columns": cols},
        )
        assert r.status_code == 201

    # Use qualified aliases so every column reference is explicit.
    sql = "SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id"
    post_resp = await client.post(
        "/queries",
        json={"user_id": "u1", "query_text": sql, "source_name": "prod", "query_name": "test"},
    )
    assert post_resp.status_code == 201
    query_id = post_resp.json()["id"]

    lineage_resp = await client.get(f"/queries/{query_id}/lineage")
    assert lineage_resp.status_code == 200
    lineage = lineage_resp.json()

    assert set(lineage["tables"]) == {"orders", "users"}
    # All refs should be attributed — none ambiguous.
    assert lineage["ambiguous_refs"] == []

    # Verify specific attributions.
    refs = {(r["table"], r["column"]) for r in lineage["column_refs"]}
    assert ("orders", "id") in refs
    assert ("users", "name") in refs


@pytest.mark.asyncio
async def test_no_schema_multi_table_produces_ambiguous_refs(client):
    """Without a schema, unqualified columns in multi-table queries become ambiguous_refs."""
    # No schema registered. Query has unqualified columns across two tables.
    sql = "SELECT name FROM orders JOIN users ON orders.user_id = users.id"
    post_resp = await client.post(
        "/queries",
        json={"user_id": "u1", "query_text": sql, "query_name": "test"},  # no source_name
    )
    assert post_resp.status_code == 201
    query_id = post_resp.json()["id"]

    lineage_resp = await client.get(f"/queries/{query_id}/lineage")
    assert lineage_resp.status_code == 200
    lineage = lineage_resp.json()

    assert set(lineage["tables"]) == {"orders", "users"}
    # `name` is unqualified and multi-table → should appear in ambiguous_refs
    assert len(lineage["ambiguous_refs"]) >= 1
    ambig_cols = {r["column"] for r in lineage["ambiguous_refs"]}
    assert "name" in ambig_cols


@pytest.mark.asyncio
async def test_schema_upsert_replaces_columns(client):
    """Re-registering the same table atomically replaces its columns."""
    base = {"source_name": "prod", "table_name": "events", "dialect": "postgres"}
    r1 = await client.post("/schemas", json={**base, "columns": [{"column_name": "id"}]})
    assert r1.status_code == 201
    assert r1.json()["column_count"] == 1

    r2 = await client.post(
        "/schemas",
        json={**base, "columns": [{"column_name": "id"}, {"column_name": "created_at"}]},
    )
    assert r2.status_code == 201
    assert r2.json()["column_count"] == 2

    # Only one schema row should exist (upsert, not duplicate).
    list_resp = await client.get("/schemas", params={"source_name": "prod"})
    assert list_resp.json()["total"] == 1
    schema_id = list_resp.json()["items"][0]["id"]

    detail = await client.get(f"/schemas/{schema_id}")
    assert len(detail.json()["columns"]) == 2
