"""schema_routes.py — HTTP endpoints for the Schema Registry.

Schemas are immutable in v0/MVP — register once, never update.
The upsert semantics of the underlying repository (atomic column replacement)
are ready for v1 when schema evolution is added; no code changes needed here
at that point, only the ``mark_queries_stale_for_source`` call needs wiring in.
"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import (
    QueriesByTableResponse,
    RegisterSchemaRequest,
    RegisterSchemaResponse,
    SchemaColumnDetail,
    SchemaDetail,
    SchemaListResponse,
    SourceListResponse,
)
from app.core.logging import get_logger
from app.db.repositories import (
    delete_table_schema_by_id,
    get_table_schema_by_id,
    list_source_names,
    list_table_schemas,
    upsert_table_schema,
)
from app.db.session import get_db_session

log = get_logger(__name__)
router = APIRouter(prefix="/schemas", tags=["schemas"])


@router.post("", response_model=RegisterSchemaResponse, status_code=201)
async def register_schema(
    payload: RegisterSchemaRequest,
    db: AsyncSession = Depends(get_db_session),
) -> RegisterSchemaResponse:
    """Register (or fully replace) a table schema within a named data source.

    Calling this endpoint for a table that already exists atomically replaces
    its column definitions — useful for correcting mistakes without deleting
    and re-creating.

    **Note (MVP)**: schemas are treated as immutable. Calling this endpoint
    after queries have been ingested will not automatically re-run lineage
    extraction for those queries. In v1, schema updates will mark affected
    queries as ``stale`` and a re-extraction endpoint will be provided.
    """
    obj = await upsert_table_schema(
        db,
        source_name=payload.source_name,
        db_database=payload.db_database,
        db_schema=payload.db_schema,
        table_name=payload.table_name,
        dialect=payload.dialect,
        columns=[c.model_dump() for c in payload.columns],
    )
    await db.commit()
    await db.refresh(obj)

    log.info(
        "schema_registered",
        schema_id=str(obj.id),
        source_name=obj.source_name,
        table_name=obj.table_name,
        column_count=len(payload.columns),
    )

    return RegisterSchemaResponse(
        id=obj.id,
        source_name=obj.source_name,
        db_database=obj.db_database,
        db_schema=obj.db_schema,
        table_name=obj.table_name,
        dialect=obj.dialect,
        column_count=len(payload.columns),
        created_at=obj.created_at,
        updated_at=obj.updated_at,
    )


@router.get("", response_model=SchemaListResponse)
async def list_schemas(
    source_name: str | None = None,
    db: AsyncSession = Depends(get_db_session),
) -> SchemaListResponse:
    """List all registered table schemas, optionally filtered by source."""
    items = await list_table_schemas(db, source_name=source_name)
    return SchemaListResponse(
        items=[
            RegisterSchemaResponse(
                id=s.id,
                source_name=s.source_name,
                db_database=s.db_database,
                db_schema=s.db_schema,
                table_name=s.table_name,
                dialect=s.dialect,
                column_count=len(s.columns),
                created_at=s.created_at,
                updated_at=s.updated_at,
            )
            for s in items
        ],
        total=len(items),
    )


@router.get("/sources", response_model=SourceListResponse)
async def list_sources(
    db: AsyncSession = Depends(get_db_session),
) -> SourceListResponse:
    """Return all distinct source_name values registered in the schema registry."""
    sources = await list_source_names(db)
    return SourceListResponse(sources=sources)


@router.get("/{schema_id}", response_model=SchemaDetail)
async def get_schema(
    schema_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
) -> SchemaDetail:
    """Fetch a single table schema with its full column definitions."""
    obj = await get_table_schema_by_id(db, schema_id=schema_id)
    if obj is None:
        log.warning("schema_not_found", schema_id=str(schema_id))
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found")
    return SchemaDetail(
        id=obj.id,
        source_name=obj.source_name,
        db_database=obj.db_database,
        db_schema=obj.db_schema,
        table_name=obj.table_name,
        dialect=obj.dialect,
        column_count=len(obj.columns),
        created_at=obj.created_at,
        updated_at=obj.updated_at,
        columns=[
            SchemaColumnDetail(
                column_name=c.column_name,
                data_type=c.data_type,
                is_nullable=c.is_nullable,
                ordinal_position=c.ordinal_position,
            )
            for c in obj.columns
        ],
    )


@router.delete("/{schema_id}", status_code=204)
async def delete_schema(
    schema_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
) -> None:
    """Delete a registered table schema and all its column definitions."""
    found = await delete_table_schema_by_id(db, schema_id=schema_id)
    if not found:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Schema not found")
    await db.commit()
    log.info("schema_deleted", schema_id=str(schema_id))
