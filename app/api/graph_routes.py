import uuid
from collections import defaultdict
from typing import Sequence, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import (
    GraphResponse,
    GraphNodeResponse,
    GraphColumnInfo,
    GraphEdgeResponse,
)
from app.db.models import GraphNode, GraphEdge, SQLQuery, TableSchema, SchemaColumn
from app.db.repositories import (
    get_graph_for_query,
    get_global_graph,
    get_sql_query_by_id,
)
from app.db.session import get_db_session

router = APIRouter(prefix="", tags=["graph"])



async def _enrich_and_build_graph_response(
    db: AsyncSession,
    nodes: Sequence[GraphNode],
    edges: Sequence[GraphEdge],
) -> GraphResponse:
    # 1. Deduplicate query IDs
    query_ids = list({n.query_id for n in nodes}.union({e.query_id for e in edges}))

    # 2. Get source_names for these queries
    if not query_ids:
        return GraphResponse(nodes=[], edges=[], query_ids=[])

    stmt = select(SQLQuery.id, SQLQuery.source_name).where(SQLQuery.id.in_(query_ids))
    res = await db.execute(stmt)
    query_to_source = {row.id: row.source_name for row in res.all()}
    source_names = list({s for s in query_to_source.values() if s})

    # 3. Fetch schema columns for these sources
    schema_map: dict[str, list[GraphColumnInfo]] = {}
    if source_names:
        schema_stmt = (
            select(TableSchema.table_name, SchemaColumn.column_name, SchemaColumn.data_type)
            .join(SchemaColumn, TableSchema.id == SchemaColumn.table_schema_id)
            .where(TableSchema.source_name.in_(source_names))
            .order_by(TableSchema.table_name, SchemaColumn.ordinal_position)
        )
        schema_res = await db.execute(schema_stmt)
        for tbl, col, dtype in schema_res.all():
            if tbl not in schema_map:
                schema_map[tbl] = []
            schema_map[tbl].append(GraphColumnInfo(name=col, data_type=dtype))

    # 4. Construct edges and observe referenced columns
    target_columns_by_node: dict[str, set[str]] = defaultdict(set)
    source_columns_by_node: dict[str, set[str]] = defaultdict(set)
    
    edge_responses = []
    for e in edges:
        edge_id = f"{e.source_node}::{e.source_column}->{e.target_node}::{e.target_column}"
        edge_responses.append(
            GraphEdgeResponse(
                id=edge_id,
                source_node_id=f"source::{e.source_node}" if e.target_node != e.source_node and e.source_node not in {n.node_name for n in nodes if n.node_type.value != "source"} else f"source::{e.source_node}", # We'll fix node IDs below
                source_column=e.source_column,
                target_node_id=f"model::{e.target_node}", # simplified
                target_column=e.target_column,
            )
        )
        if e.source_column:
            source_columns_by_node[e.source_node].add(e.source_column)
        if e.target_column:
            target_columns_by_node[e.target_node].add(e.target_column)

    # Re-map proper node type IDs
    node_type_map = {n.node_name: n.node_type.value for n in nodes}
    for er in edge_responses:
        s_type = node_type_map.get(er.source_node_id.replace("source::", ""), "source")
        t_type = node_type_map.get(er.target_node_id.replace("model::", ""), "model")
        er.source_node_id = f"{s_type}::{er.source_node_id.split('::')[-1]}"
        er.target_node_id = f"{t_type}::{er.target_node_id.split('::')[-1]}"
        er.id = f"{er.source_node_id}::{er.source_column}->{er.target_node_id}::{er.target_column}"

    # 5. Build nodes
    node_responses = []
    for n in nodes:
        node_id = f"{n.node_type.value}::{n.node_name}"
        cols: list[GraphColumnInfo] = []
        
        seen_cols = set()
        if n.node_type.value == "source":
            # Add schema columns first
            for sc in schema_map.get(n.node_name, []):
                cols.append(sc)
                seen_cols.add(sc.name)
            
            # Add observed source columns
            for oc in source_columns_by_node.get(n.node_name, set()):
                if oc not in seen_cols and oc != "*":
                    cols.append(GraphColumnInfo(name=oc))
                    seen_cols.add(oc)
        else:
            # For models/CTEs, we list observed target columns
            for oc in target_columns_by_node.get(n.node_name, set()):
                if oc not in seen_cols and oc != "*":
                    cols.append(GraphColumnInfo(name=oc))
                    seen_cols.add(oc)

        node_responses.append(
            GraphNodeResponse(
                id=node_id,
                node_name=n.node_name,
                node_type=n.node_type.value,
                columns=cols,
            )
        )

    return GraphResponse(
        nodes=node_responses,
        edges=edge_responses,
        query_ids=query_ids,
    )


@router.get("/queries/{query_id}/graph", response_model=GraphResponse)
async def get_query_graph(
    query_id: uuid.UUID,
    db: AsyncSession = Depends(get_db_session),
):
    """Get the table and column-level lineage graph for a specific query."""
    query = await get_sql_query_by_id(db, query_id=query_id)
    if not query:
        raise HTTPException(status_code=404, detail="Query not found")

    nodes, edges = await get_graph_for_query(db, query_id)
    return await _enrich_and_build_graph_response(db, nodes, edges)


@router.get("/graph", response_model=GraphResponse)
async def get_global_graph_endpoint(
    source_name: str | None = Query(None, description="Filter to a specific logical source"),
    node_name: str | None = Query(None, description="Filter to a specific node subgraph"),
    limit: int = 500,
    db: AsyncSession = Depends(get_db_session),
):
    """Get the global cross-query lineage graph."""
    nodes, edges = await get_global_graph(
        db,
        source_name=source_name,
        node_name=node_name,
        limit=limit,
    )
    return await _enrich_and_build_graph_response(db, nodes, edges)
