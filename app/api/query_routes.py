from __future__ import annotations

from datetime import datetime
import json
import uuid
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import (
    AmbiguousRef,
    ColumnRef,
    CreateQueryRequest,
    CreateQueryResponse,
    LineageResponse,
    QueriesByTableResponse,
    QueryListResponse,
    QueryResponse,
    QuerySummary,
    TableListResponse,
)
from app.core.logging import get_logger
from app.db.models import LineageType
from app.db.repositories import (
    count_sql_queries,
    get_sql_query_by_id,
    list_all_table_names,
    list_queries_by_table,
    list_query_lineage_rows,
    list_sql_queries,
)
from app.db.session import get_db_session
from app.services.query_service import ingest_query

log = get_logger(__name__)
router = APIRouter(tags=["queries"])


# ── Query endpoints ────────────────────────────────────────────────────────────


@router.post("/queries", response_model=CreateQueryResponse, status_code=201)
async def create_query(
    payload: CreateQueryRequest,
    db: AsyncSession = Depends(get_db_session),
) -> CreateQueryResponse:
    """Store a SQL query and synchronously extract its lineage.

    Lineage extraction failures are recorded on the row but never cause a
    non-201 response — see ``lineage_status`` and ``parse_error`` fields.
    """
    obj = await ingest_query(
        db,
        user_id=payload.user_id,
        query_text=payload.query_text,
        query_name=payload.query_name,
        tags=payload.tags,
        source_name=payload.source_name,
    )
    return CreateQueryResponse(
        id=obj.id,
        user_id=obj.user_id,
        created_at=obj.created_at,
        lineage_status=obj.lineage_status.value,
    )


@router.get("/queries", response_model=QueryListResponse)
async def list_queries(
    user_id: str | None = None,
    created_after: datetime | None = Query(default=None, alias="created_after"),
    table_name: str | None = None,
    tags: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db_session),
) -> QueryListResponse:
    """List stored queries with optional filters.

    Filters:
    - ``user_id``: exact match on the stored user identifier.
    - ``created_after``: ISO-8601 timestamp; returns queries created at or
      after this time.
    - ``table_name``: only queries whose lineage references this table.
    - ``tags``: JSON object; returned queries must contain these key-value
      pairs in their ``tags`` field (JSONB containment on Postgres).

    Response includes ``total`` — the count of all matching rows ignoring
    ``limit``/``offset`` — so clients can implement pagination correctly.
    """
    tags_dict: Dict[str, Any] | None = None
    if tags is not None:
        try:
            tags_dict = json.loads(tags)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="tags must be valid JSON",
            ) from exc

    filter_kwargs: Dict[str, Any] = dict(
        user_id=user_id,
        created_after=created_after,
        table_name=table_name,
        tags=tags_dict,
    )

    items, total = await _list_and_count(db, filter_kwargs=filter_kwargs, limit=limit, offset=offset)

    log.info("queries_listed", total=total, limit=limit, offset=offset, **{k: v for k, v in filter_kwargs.items() if v is not None})

    return QueryListResponse(
        items=[
            {
                "id": item.id,
                "user_id": item.user_id,
                "query_text": item.query_text,
                "query_name": item.query_name,
                "tags": item.tags,
                "source_name": item.source_name,
                "created_at": item.created_at,
                "updated_at": item.updated_at,
                "lineage_status": item.lineage_status.value,
            }
            for item in items
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/queries/{query_id}", response_model=QueryResponse)
async def get_query(
    query_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
) -> QueryResponse:
    """Fetch a single query by ID."""
    obj = await get_sql_query_by_id(db, query_id=query_id)
    if obj is None:
        log.warning("query_not_found", query_id=str(query_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not found")
    return QueryResponse(
        id=obj.id,
        user_id=obj.user_id,
        query_text=obj.query_text,
        query_name=obj.query_name,
        tags=obj.tags,
        source_name=obj.source_name,
        created_at=obj.created_at,
        updated_at=obj.updated_at,
        lineage_status=obj.lineage_status.value,
        parse_error=obj.parse_error,
    )


@router.get("/queries/{query_id}/lineage", response_model=LineageResponse)
async def get_lineage(
    query_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
) -> LineageResponse:
    """Return the extracted lineage for a stored query.

    - ``tables``: physical tables referenced by the query (CTEs excluded).
    - ``columns``: referenced column names (deduplicated).
    - ``column_lineage``: mapping of column name → list of source tables.

    Returns empty lists for queries whose lineage extraction failed.
    """
    obj = await get_sql_query_by_id(db, query_id=query_id)
    if obj is None:
        log.warning("query_not_found", query_id=str(query_id), endpoint="lineage")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Query not found")

    rows = await list_query_lineage_rows(db, query_id=query_id)

    tables: list[str] = []
    column_refs: list[ColumnRef] = []
    ambiguous: dict[str, list[str]] = {}  # col_name -> [candidate_table, ...]
    seen_tables: set[str] = set()
    seen_col_keys: set[tuple[str, str]] = set()

    for row in rows:
        # CTE names are not physical tables — skip them in the response.
        if row.lineage_type == LineageType.cte:
            continue

        if row.lineage_type == LineageType.ambiguous:
            # Accumulate candidate tables for this column name.
            col = row.column_name or ""
            ambiguous.setdefault(col, []).append(row.table_name)
            continue

        if row.column_name is None:
            # Table-level reference (no specific column).
            if row.table_name not in seen_tables:
                seen_tables.add(row.table_name)
                tables.append(row.table_name)
        else:
            key = (row.table_name, row.column_name)
            if key not in seen_col_keys:
                seen_col_keys.add(key)
                column_refs.append(
                    ColumnRef(
                        table=row.table_name,
                        column=row.column_name,
                        lineage_type=row.lineage_type.value,
                    )
                )

    ambiguous_refs = [
        AmbiguousRef(column=col, candidate_tables=tables_list)
        for col, tables_list in ambiguous.items()
    ]

    return LineageResponse(
        tables=tables,
        column_refs=column_refs,
        ambiguous_refs=ambiguous_refs,
    )


# ── Inverse lineage endpoints ──────────────────────────────────────────────────


@router.get("/lineage/tables", response_model=TableListResponse)
async def list_tables(
    db: AsyncSession = Depends(get_db_session),
) -> TableListResponse:
    """Return every distinct table name referenced across all stored queries.

    This is the inverse-lineage lookup: "what tables does this service know
    about?" — useful for autocomplete, data cataloguing, or impact analysis.
    """
    table_names = await list_all_table_names(db)
    log.info("lineage_tables_listed", count=len(table_names))
    return TableListResponse(tables=table_names)


@router.get("/lineage/tables/{table_name}", response_model=QueriesByTableResponse)
async def queries_by_table(
    table_name: str,
    db: AsyncSession = Depends(get_db_session),
) -> QueriesByTableResponse:
    """Return all queries whose lineage references *table_name*.

    Enables impact analysis: "if I change the schema of this table, which
    queries will be affected?"
    """
    queries = await list_queries_by_table(db, table_name=table_name)
    log.info("lineage_queries_by_table", table_name=table_name, count=len(queries))
    return QueriesByTableResponse(
        table_name=table_name,
        queries=[
            QuerySummary(
                id=q.id,
                user_id=q.user_id,
                query_name=q.query_name,
                created_at=q.created_at,
                lineage_status=q.lineage_status.value,
            )
            for q in queries
        ],
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _list_and_count(db: AsyncSession, *, filter_kwargs: Dict[str, Any], limit: int, offset: int):
    """Run list + count queries concurrently and return both results.

    Keeping this as a helper avoids duplicating the filter kwargs at every
    call site and makes it easy to swap to a single SQL window-function query
    in v1.
    """
    items = await list_sql_queries(db, **filter_kwargs, limit=limit, offset=offset)
    total = await count_sql_queries(db, **filter_kwargs)
    return items, total

