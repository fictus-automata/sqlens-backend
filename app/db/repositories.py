from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
import json
from typing import Any
import uuid

from sqlalchemy import Select, func, select, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import LineageStatus, QueryLineage, SQLQuery, SchemaColumn, TableSchema, GraphNode, GraphEdge
from app.services.graph_service import ExtractedGraph


async def create_sql_query(
    db: AsyncSession,
    *,
    user_id: str,
    query_text: str,
    query_name: str | None,
    tags: dict[str, Any] | None,
    source_name: str | None = None,
) -> SQLQuery:
    obj = SQLQuery(
        user_id=user_id,
        query_text=query_text,
        query_name=query_name,
        tags=tags,
        source_name=source_name,
    )
    db.add(obj)
    await db.flush()  # assign UUID before returning
    return obj


async def get_sql_query_by_id(db: AsyncSession, *, query_id: uuid.UUID) -> SQLQuery | None:
    stmt: Select[tuple[SQLQuery]] = select(SQLQuery).where(SQLQuery.id == query_id)
    res = await db.execute(stmt)
    return res.scalar_one_or_none()


def _apply_query_filters(
    stmt,
    *,
    user_id: str | None,
    created_after: datetime | None,
    table_name: str | None,
    tags: dict[str, Any] | None,
):
    """Apply the shared filter set to a SELECT or COUNT statement.

    Extracted to avoid duplicating filter logic between ``list_sql_queries``
    and ``count_sql_queries``.
    """
    if user_id is not None:
        stmt = stmt.where(SQLQuery.user_id == user_id)
    if created_after is not None:
        stmt = stmt.where(SQLQuery.created_at >= created_after)
    if tags is not None:
        # JSONB containment (Postgres). On SQLite this may not behave identically,
        # but it is kept for plan parity.
        stmt = stmt.where(SQLQuery.tags.contains(tags))  # type: ignore[arg-type]
    if table_name is not None:
        stmt = stmt.join(QueryLineage, QueryLineage.query_id == SQLQuery.id).where(
            QueryLineage.table_name == table_name
        )
    return stmt


async def list_sql_queries(
    db: AsyncSession,
    *,
    user_id: str | None,
    created_after: datetime | None,
    table_name: str | None,
    tags: dict[str, Any] | None,
    limit: int,
    offset: int,
) -> Sequence[SQLQuery]:
    stmt = select(SQLQuery)
    stmt = _apply_query_filters(stmt, user_id=user_id, created_after=created_after, table_name=table_name, tags=tags)
    stmt = stmt.order_by(SQLQuery.created_at.desc()).limit(limit).offset(offset)
    res = await db.execute(stmt)
    # De-duplicate rows produced when the join on query_lineage is used.
    return list({q.id: q for q in res.scalars().all()}.values())


async def count_sql_queries(
    db: AsyncSession,
    *,
    user_id: str | None,
    created_after: datetime | None,
    table_name: str | None,
    tags: dict[str, Any] | None,
) -> int:
    """Return the total number of queries matching the same filters as list_sql_queries."""
    stmt = select(func.count()).select_from(SQLQuery)
    stmt = _apply_query_filters(stmt, user_id=user_id, created_after=created_after, table_name=table_name, tags=tags)
    res = await db.execute(stmt)
    return res.scalar_one()


async def list_all_table_names(db: AsyncSession) -> list[str]:
    """Return all distinct table names referenced across all stored queries."""
    from sqlalchemy import distinct

    stmt = select(distinct(QueryLineage.table_name)).order_by(QueryLineage.table_name)
    res = await db.execute(stmt)
    return list(res.scalars().all())


async def list_queries_by_table(db: AsyncSession, *, table_name: str) -> list[SQLQuery]:
    """Return all queries that reference *table_name* in their extracted lineage."""
    stmt = (
        select(SQLQuery)
        .join(QueryLineage, QueryLineage.query_id == SQLQuery.id)
        .where(QueryLineage.table_name == table_name)
        .distinct()
        .order_by(SQLQuery.created_at.desc())
    )
    res = await db.execute(stmt)
    return list(res.scalars().all())


async def list_query_lineage_rows(db: AsyncSession, *, query_id: uuid.UUID) -> list[QueryLineage]:
    stmt = select(QueryLineage).where(QueryLineage.query_id == query_id).order_by(QueryLineage.created_at.asc())
    res = await db.execute(stmt)
    return list(res.scalars().all())


# ── Schema Registry repository functions ──────────────────────────────────────


async def upsert_table_schema(
    db: AsyncSession,
    *,
    source_name: str,
    db_database: str | None,
    db_schema: str | None,
    table_name: str,
    dialect: str,
    columns: list[dict[str, Any]],
) -> TableSchema:
    """Insert or update a table schema.

    Uses upsert semantics: if a schema already exists for the
    ``(source_name, db_database, db_schema, table_name)`` combination, its
    columns are **atomically replaced** (old columns deleted, new ones
    inserted). The parent row's ``updated_at`` timestamp is refreshed.

    Args:
        columns: List of dicts with keys ``column_name`` (required),
            ``data_type``, ``is_nullable``, ``ordinal_position`` (all optional).
    """
    # Try to find an existing schema row.
    stmt = select(TableSchema).where(
        TableSchema.source_name == source_name,
        TableSchema.table_name == table_name,
    )
    stmt = stmt.where(
        TableSchema.db_database == db_database if db_database is not None else TableSchema.db_database.is_(None)
    )
    stmt = stmt.where(
        TableSchema.db_schema == db_schema if db_schema is not None else TableSchema.db_schema.is_(None)
    )

    res = await db.execute(stmt)
    obj = res.scalar_one_or_none()

    if obj is None:
        obj = TableSchema(
            source_name=source_name,
            db_database=db_database,
            db_schema=db_schema,
            table_name=table_name,
            dialect=dialect,
        )
        db.add(obj)
        await db.flush()
    else:
        obj.dialect = dialect
        # Atomically replace columns: delete existing, then insert new.
        await db.execute(delete(SchemaColumn).where(SchemaColumn.table_schema_id == obj.id))

    for col in columns:
        db.add(
            SchemaColumn(
                table_schema_id=obj.id,
                column_name=col["column_name"],
                data_type=col.get("data_type"),
                is_nullable=col.get("is_nullable"),
                ordinal_position=col.get("ordinal_position"),
            )
        )

    return obj


async def get_table_schema_by_id(db: AsyncSession, *, schema_id: uuid.UUID) -> TableSchema | None:
    """Fetch a single table schema with its columns eagerly loaded."""
    stmt = (
        select(TableSchema)
        .options(selectinload(TableSchema.columns))
        .where(TableSchema.id == schema_id)
    )
    res = await db.execute(stmt)
    return res.scalar_one_or_none()


async def list_table_schemas(
    db: AsyncSession,
    *,
    source_name: str | None = None,
) -> list[TableSchema]:
    """List all registered table schemas, optionally filtered by source."""
    stmt = (
        select(TableSchema)
        .options(selectinload(TableSchema.columns))
        .order_by(TableSchema.source_name, TableSchema.table_name)
    )
    if source_name is not None:
        stmt = stmt.where(TableSchema.source_name == source_name)
    res = await db.execute(stmt)
    return list(res.scalars().unique().all())


async def delete_table_schema_by_id(db: AsyncSession, *, schema_id: uuid.UUID) -> bool:
    """Delete a table schema (and cascade to its columns). Returns True if found."""
    stmt = delete(TableSchema).where(TableSchema.id == schema_id)
    res = await db.execute(stmt)
    return res.rowcount > 0


async def load_schema_for_source(db: AsyncSession, *, source_name: str) -> list[TableSchema]:
    """Load all table schemas (with columns) for a given source.

    Called at query ingest time to build the sqlglot schema dict for
    ``qualify_columns``.
    """
    stmt = (
        select(TableSchema)
        .options(selectinload(TableSchema.columns))
        .where(TableSchema.source_name == source_name)
    )
    res = await db.execute(stmt)
    return list(res.scalars().unique().all())


async def list_source_names(db: AsyncSession) -> list[str]:
    """Return all distinct source_name values across registered schemas."""
    from sqlalchemy import distinct

    stmt = select(distinct(TableSchema.source_name)).order_by(TableSchema.source_name)
    res = await db.execute(stmt)
    return list(res.scalars().all())



    # v1: add mark_queries_stale_for_source(db, source_name) here once schema
    # updates are supported and lineage re-extraction is needed.


# ── Graph Extractors ──────────────────────────────────────────────────────────


async def save_graph(db: AsyncSession, query_id: uuid.UUID, graph: ExtractedGraph) -> None:
    """Upsert nodes and edges for a query. Deletes existing rows first."""
    # Delete existing
    await db.execute(delete(GraphEdge).where(GraphEdge.query_id == query_id))
    await db.execute(delete(GraphNode).where(GraphNode.query_id == query_id))

    if not graph.nodes and not graph.edges:
        return

    # Bulk insert
    if graph.nodes:
        db.add_all([
            GraphNode(
                query_id=query_id,
                node_name=n.node_name,
                node_type=n.node_type,
            )
            for n in graph.nodes
        ])

    if graph.edges:
        db.add_all([
            GraphEdge(
                query_id=query_id,
                source_node=e.source_node,
                source_column=e.source_column,
                target_node=e.target_node,
                target_column=e.target_column,
            )
            for e in graph.edges
        ])

    await db.flush()


async def get_graph_for_query(db: AsyncSession, query_id: uuid.UUID) -> tuple[Sequence[GraphNode], Sequence[GraphEdge]]:
    """Get all graph nodes and edges for a specific query."""
    node_stmt = select(GraphNode).where(GraphNode.query_id == query_id).order_by(GraphNode.node_name)
    nodes = (await db.execute(node_stmt)).scalars().all()

    edge_stmt = select(GraphEdge).where(GraphEdge.query_id == query_id).order_by(
        GraphEdge.source_node, GraphEdge.source_column, GraphEdge.target_node, GraphEdge.target_column
    )
    edges = (await db.execute(edge_stmt)).scalars().all()

    return nodes, edges


async def get_global_graph(
    db: AsyncSession,
    source_name: str | None = None,
    node_name: str | None = None,
    limit: int = 500,
) -> tuple[Sequence[GraphNode], Sequence[GraphEdge]]:
    """Get global graph aggregating all queries.
    
    If source_name is provided, filters to queries that have this source_name.
    """
    node_stmt = select(GraphNode)
    edge_stmt = select(GraphEdge)

    if source_name:
        node_stmt = node_stmt.join(SQLQuery, GraphNode.query_id == SQLQuery.id).where(SQLQuery.source_name == source_name)
        edge_stmt = edge_stmt.join(SQLQuery, GraphEdge.query_id == SQLQuery.id).where(SQLQuery.source_name == source_name)

    # Simplified node filter: only fetch nodes with node_name
    if node_name:
        node_stmt = node_stmt.where(GraphNode.node_name == node_name)
        # We need edges connected to this node
        edge_stmt = edge_stmt.where(
            (GraphEdge.source_node == node_name) | (GraphEdge.target_node == node_name)
        )

    # For a real graph query we might only return nodes connected by the edges we fetched.
    # To keep it bounded:
    nodes = (await db.execute(node_stmt.order_by(GraphNode.id).limit(limit))).scalars().all()
    
    if node_name:
        # Fetch the nodes connected via edges too
        connected_node_names = set()
        edges = (await db.execute(edge_stmt)).scalars().all()
        for e in edges:
            connected_node_names.add(e.source_node)
            connected_node_names.add(e.target_node)
        
        if connected_node_names:
            extra_node_stmt = select(GraphNode).where(GraphNode.node_name.in_(connected_node_names))
            if source_name:
                extra_node_stmt = extra_node_stmt.join(SQLQuery, GraphNode.query_id == SQLQuery.id).where(SQLQuery.source_name == source_name)
            extra_nodes = (await db.execute(extra_node_stmt)).scalars().all()
            
            # merge uniquely by id
            node_map = {n.id: n for n in nodes}
            for en in extra_nodes:
                node_map[en.id] = en
            nodes = list(node_map.values())
    else:
        edges = (await db.execute(edge_stmt.order_by(GraphEdge.id).limit(limit * 2))).scalars().all()

    return nodes, edges

