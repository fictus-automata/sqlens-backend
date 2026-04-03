import asyncio
import importlib
import os
from datetime import datetime, timedelta
import uuid

import httpx
import pytest
import pytest_asyncio


@pytest_asyncio.fixture()
async def client(tmp_path):
    db_path = tmp_path / "altimate_test.db"
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"

    # Reload modules that read DATABASE_URL at import time.
    import app.core.config as config_module
    import app.db.models as models_module
    import app.db.session as session_module
    import app.api.query_routes as routes_module
    import app.main as main_module

    importlib.reload(config_module)
    importlib.reload(models_module)
    importlib.reload(session_module)
    await session_module.init_db()
    importlib.reload(routes_module)
    import app.api.schema_routes as schema_routes_module
    importlib.reload(schema_routes_module)
    importlib.reload(main_module)

    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_post_and_get_queries(client):
    post_resp = await client.post("/queries", json={"user_id": "u1", "query_text": "SELECT col1 FROM table1", "query_name": "test"})
    assert post_resp.status_code == 201
    post_data = post_resp.json()
    query_id = uuid.UUID(post_data["id"])

    get_list_resp = await client.get("/queries", params={"user_id": "u1", "limit": 10, "offset": 0})
    assert get_list_resp.status_code == 200
    data = get_list_resp.json()
    assert len(data["items"]) == 1
    assert uuid.UUID(data["items"][0]["id"]) == query_id
    assert data["items"][0]["query_text"] == "SELECT col1 FROM table1"

    get_query_resp = await client.get(f"/queries/{query_id}")
    assert get_query_resp.status_code == 200
    get_query = get_query_resp.json()
    assert get_query["lineage_status"] == "completed"
    assert get_query["parse_error"] is None

    lineage_resp = await client.get(f"/queries/{query_id}/lineage")
    assert lineage_resp.status_code == 200
    lineage = lineage_resp.json()
    assert lineage["tables"] == ["table1"]
    assert lineage["column_refs"] == [
        {"table": "table1", "column": "col1", "lineage_type": "source"}
    ]
    assert lineage["ambiguous_refs"] == []


@pytest.mark.asyncio
async def test_created_after_filter(client):
    post_1 = await client.post("/queries", json={"user_id": "u1", "query_text": "SELECT col1 FROM table1", "query_name": "test1"})
    assert post_1.status_code == 201
    created_at = post_1.json()["created_at"]

    # Ensure at least 1s between timestamps for sqlite.
    await asyncio.sleep(1.05)

    await client.post("/queries", json={"user_id": "u1", "query_text": "SELECT col2 FROM table2", "query_name": "test2"})

    created_at_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    created_after = (created_at_dt + timedelta(microseconds=1000)).isoformat()

    filtered = await client.get(
        "/queries",
        params={"user_id": "u1", "created_after": created_after, "limit": 10, "offset": 0},
    )
    assert filtered.status_code == 200
    items = filtered.json()["items"]
    assert len(items) == 1
    assert items[0]["query_text"] == "SELECT col2 FROM table2"


@pytest.mark.asyncio
async def test_parse_failure_does_not_block_ingest(client):
    post_resp = await client.post("/queries", json={"user_id": "u1", "query_text": "UPDATE table1 SET x = 1", "query_name": "test"})
    assert post_resp.status_code == 201
    query_id = uuid.UUID(post_resp.json()["id"])

    query_resp = await client.get(f"/queries/{query_id}")
    assert query_resp.status_code == 200
    query = query_resp.json()
    assert query["lineage_status"] == "failed"
    assert query["parse_error"] is not None

    lineage_resp = await client.get(f"/queries/{query_id}/lineage")
    assert lineage_resp.status_code == 200
    lineage = lineage_resp.json()
    assert lineage["tables"] == []
    assert lineage["column_refs"] == []
    assert lineage["ambiguous_refs"] == []

