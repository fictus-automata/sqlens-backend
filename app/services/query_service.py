"""query_service.py — Business logic for SQL query ingest and lineage extraction.

HTTP routes delegate here; this module owns:
  1. Persisting the SQL query record.
  2. Optionally loading registered schemas for the query's source.
  3. Running synchronous lineage extraction (v0), schema-aware when available.
  4. Persisting extracted lineage rows.
  5. Handling parse failures without failing the ingest — POST /queries
     always returns HTTP 201; failures are surfaced through
     ``lineage_status=failed`` and the ``parse_error`` field.
"""
from __future__ import annotations

from typing import Any

from sqlglot.errors import ParseError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logging import get_logger
from app.db.models import LineageStatus, LineageType, QueryLineage, SQLQuery
from app.db.repositories import create_sql_query, load_schema_for_source
from app.services.lineage_service import compute_lineage
from app.services.schema_service import build_sqlglot_schema

log = get_logger(__name__)


async def ingest_query(
    db: AsyncSession,
    *,
    user_id: str,
    query_text: str,
    query_name: str | None,
    tags: dict[str, Any] | None,
    source_name: str | None = None,
) -> SQLQuery:
    """Store a SQL query and synchronously extract + persist its lineage.

    When *source_name* is provided and schemas have been registered for that
    source, the registered column definitions are used to fully qualify every
    column reference before extraction via
    ``sqlglot.optimizer.qualify_columns``.  This eliminates ambiguity when
    identical column names exist across tables (e.g. ``orders.id`` vs
    ``users.id``).

    Without a schema, unqualified columns in multi-table queries are stored
    with ``lineage_type="ambiguous"`` and grouped into ``ambiguous_refs`` in
    the lineage response instead of being silently misattributed.

    Lineage extraction failures (parse errors, unexpected exceptions) are
    recorded on the query row via ``lineage_status`` and ``parse_error`` but
    are **never re-raised** — the HTTP ingest always succeeds.

    Args:
        db: Active async database session.
        user_id: Client-provided user identifier (not validated — no auth in v0).
        query_text: The raw SQL query string.
        query_name: Optional human-friendly label for the query.
        tags: Arbitrary JSON metadata to attach to the query.
        source_name: Optional logical data-source identifier.  When set,
            registered schemas for this source are loaded and used for
            column qualification.

    Returns:
        The persisted :class:`~app.db.models.SQLQuery` ORM object, refreshed
        after commit so all server-generated fields (timestamps, status) are
        populated.
    """
    obj = await create_sql_query(
        db,
        user_id=user_id,
        query_text=query_text,
        query_name=query_name,
        tags=tags,
        source_name=source_name,
    )
    log.info("query_ingested", query_id=str(obj.id), user_id=user_id, source_name=source_name)

    # Build sqlglot schema dict if a source_name was provided.
    schema_dict: dict | None = None
    if source_name:
        table_schemas = await load_schema_for_source(db, source_name=source_name)
        if table_schemas:
            schema_dict = build_sqlglot_schema(table_schemas)
            log.info(
                "schema_loaded_for_source",
                query_id=str(obj.id),
                source_name=source_name,
                table_count=len(table_schemas),
            )
        else:
            log.info(
                "no_schemas_registered_for_source",
                query_id=str(obj.id),
                source_name=source_name,
            )

    try:
        extracted = compute_lineage(query_text, dialect=settings.sqlglot_dialect, schema=schema_dict)
        lineage_entries = extracted.get("lineage_entries", [])

        rows: list[QueryLineage] = [
            QueryLineage(
                query_id=obj.id,
                table_name=e["table_name"],
                column_name=e.get("column_name"),
                lineage_type=LineageType(e.get("lineage_type", LineageType.source.value)),
            )
            for e in lineage_entries
        ]
        db.add_all(rows)
        
        if query_name:
            from app.services.graph_service import extract_graph
            from app.db.repositories import save_graph
            
            graph = extract_graph(
                sql=query_text,
                query_name=query_name,
                dialect=settings.sqlglot_dialect,
                schema=schema_dict,
            )
            await save_graph(db, obj.id, graph)

        obj.lineage_status = LineageStatus.completed
        obj.parse_error = None
        log.info(
            "lineage_extracted",
            query_id=str(obj.id),
            tables=extracted.get("tables"),
            columns=extracted.get("columns"),
            entry_count=len(rows),
            schema_aware=schema_dict is not None,
        )


    except (ValueError, ParseError) as exc:
        obj.lineage_status = LineageStatus.failed
        obj.parse_error = str(exc)
        log.warning("lineage_extraction_failed", query_id=str(obj.id), reason=str(exc))

    except Exception as exc:
        obj.lineage_status = LineageStatus.failed
        obj.parse_error = f"lineage_failed: {exc}"
        log.exception("lineage_extraction_unexpected_error", query_id=str(obj.id))

    await db.commit()
    await db.refresh(obj)
    return obj
