from __future__ import annotations

import dataclasses
from collections import defaultdict
from typing import Any, Dict

from sqlglot import exp, parse
from sqlglot.errors import ParseError

from app.core.logging import get_logger
from app.db.models import LineageType
from app.services.lineage_service import (
    _try_qualify,
    _get_cte_names,
    _extract_alias_to_table,
    _extract_projection_has_star,
    _get_scope_tables_for_col,
    _dedupe_preserve_order,
)

log = get_logger(__name__)


def _expand_star_columns(table_name: str, schema: dict) -> list[str] | None:
    def _is_col_map(d: dict) -> bool:
        if not d:
            return True
        first = next(iter(d.values()))
        return first is None or isinstance(first, str)

    if table_name in schema and isinstance(schema[table_name], dict):
        if _is_col_map(schema[table_name]):
            return list(schema[table_name].keys())

    for ns_val in schema.values():
        if not isinstance(ns_val, dict):
            continue
        if table_name in ns_val and isinstance(ns_val[table_name], dict):
            if _is_col_map(ns_val[table_name]):
                return list(ns_val[table_name].keys())

    for db_val in schema.values():
        if not isinstance(db_val, dict):
            continue
        for schema_val in db_val.values():
            if not isinstance(schema_val, dict):
                continue
            if table_name in schema_val and isinstance(schema_val[table_name], dict):
                return list(schema_val[table_name].keys())

    return None



@dataclasses.dataclass
class GraphNodeData:
    node_name: str
    node_type: LineageType
    columns: list[str]


@dataclasses.dataclass
class GraphEdgeData:
    source_node: str
    source_column: str | None
    target_node: str
    target_column: str | None


@dataclasses.dataclass
class ExtractedGraph:
    nodes: list[GraphNodeData]
    edges: list[GraphEdgeData]


def _resolve_select_list(
    select_expr: exp.Select,
    target_model: str,
    alias_to_table: dict[str, str],
    cte_names: set[str],
    schema: dict | None,
) -> tuple[list[GraphNodeData], list[GraphEdgeData]]:
    """Extract column-level edges from a single SELECT node's projection list."""
    edges: list[GraphEdgeData] = []
    source_nodes: dict[str, set[str]] = defaultdict(set)
    target_cols: list[str] = []

    # Get the physical tables in scope for this SELECT
    from_ = select_expr.args.get("from_")
    scope_tables: list[str] = []
    if from_:
        for t in from_.find_all(exp.Table):
            real = t.name
            if real and str(real) not in cte_names:
                scope_tables.append(str(real))
    for join in select_expr.args.get("joins") or []:
        for t in join.find_all(exp.Table):
            real = t.name
            if real and str(real) not in cte_names:
                scope_tables.append(str(real))
    
    physical_tables = _dedupe_preserve_order(scope_tables)

    if _extract_projection_has_star(select_expr):
        if schema:
            for tbl in physical_tables:
                tbl_cols = _expand_star_columns(tbl, schema)
                if tbl_cols:
                    for col_name in tbl_cols:
                        edges.append(
                            GraphEdgeData(
                                source_node=tbl,
                                source_column=col_name,
                                target_node=target_model,
                                target_column=col_name,
                            )
                        )
                        source_nodes[tbl].add(col_name)
                        target_cols.append(col_name)
                else:
                    edges.append(
                        GraphEdgeData(
                            source_node=tbl,
                            source_column="*",
                            target_node=target_model,
                            target_column="*",
                        )
                    )
                    source_nodes[tbl].add("*")
                    target_cols.append("*")
        else:
            for tbl in physical_tables:
                edges.append(
                    GraphEdgeData(
                        source_node=tbl,
                        source_column="*",
                        target_node=target_model,
                        target_column="*",
                    )
                )
                source_nodes[tbl].add("*")
            target_cols.append("*")
        
        # If there are explicit columns alongside *, sqlglot puts them in select_expr.expressions
        # We will process them below as well

    for i, expr in enumerate(select_expr.expressions):
        # find target column name
        if isinstance(expr, exp.Alias):
            output_col = str(expr.alias)
        elif isinstance(expr, exp.Column):
            output_col = str(expr.name)
        elif isinstance(expr, exp.Star):
            continue  # Already handled above
        else:
            output_col = f"col_{i}"
            
        target_cols.append(output_col)
        
        # Walk expr for source columns
        for col in expr.find_all(exp.Column):
            if not col.name:
                continue
            col_name = str(col.name)
            qualifier = str(col.table) if col.table else None
            
            if qualifier:
                source_node = alias_to_table.get(qualifier, qualifier)
            else:
                # Unqualified column
                col_scope = _get_scope_tables_for_col(col, cte_names)
                if len(col_scope) == 0:
                    # e.g., reading exactly from a CTE, treated as model node edge
                    # We need to find the specific CTE if it's uniquely resolving.
                    # For simplicity in graph edge (which requires source nodes)
                    # if the ONLY FROM source is a CTE, we map the edge from the CTE.
                    # Wait, if it's from a CTE, we should emit an edge from that CTE!
                    cte_sources = []
                    # Let's see what's in from/joins that are CTEs
                    if from_:
                        for t in from_.find_all(exp.Table):
                            if str(t.name) in cte_names:
                                cte_sources.append(str(t.name))
                    for join in select_expr.args.get("joins") or []:
                        for t in join.find_all(exp.Table):
                            if str(t.name) in cte_names:
                                cte_sources.append(str(t.name))
                    
                    if len(cte_sources) == 1:
                        source_node = cte_sources[0]
                    else:
                        continue # ambiguous or empty
                elif len(col_scope) == 1:
                    source_node = col_scope[0]
                else:
                    # Ambigous ref - skip for now or we could emit for all. The edge needs a single source.
                    continue
            
            # Emit edge
            edges.append(
                GraphEdgeData(
                    source_node=source_node,
                    source_column=col_name,
                    target_node=target_model,
                    target_column=output_col,
                )
            )
            # We track the column for model nodes (but for physical sources, it'll just be added to source_nodes tracking)
            # Physical sources
            if source_node not in cte_names:
                source_nodes[source_node].add(col_name)
    
    nodes: list[GraphNodeData] = []
    for src, cols in source_nodes.items():
        if src not in cte_names:
            nodes.append(
                GraphNodeData(
                    node_name=src,
                    node_type=LineageType.source,
                    columns=_dedupe_preserve_order(list(cols)),
                )
            )
            
    # For target model node, we don't emit it here as it will be emitted by the caller with target_cols
    
    return nodes, edges


def extract_graph(
    sql: str,
    query_name: str,
    dialect: str = "postgres",
    schema: dict | None = None,
) -> ExtractedGraph:
    """Extract nodes and column-level edges from SQL."""
    if sql is None:
        raise ValueError("sql must be provided")

    sql = sql.strip()
    if not sql:
        raise ValueError("sql must be non-empty")

    try:
        statements = parse(sql, read=dialect)
    except ParseError as e:
        raise ValueError("Unsupported SQL: could not parse statement") from e

    if not statements:
        raise ValueError("Unsupported SQL: could not parse statement")

    all_nodes_map: dict[str, GraphNodeData] = {}
    all_edges: list[GraphEdgeData] = []

    def merge_nodes(nodes: list[GraphNodeData]) -> None:
        for node in nodes:
            if node.node_name in all_nodes_map:
                existing = all_nodes_map[node.node_name]
                all_cols = existing.columns + node.columns
                existing.columns = _dedupe_preserve_order(all_cols)
            else:
                all_nodes_map[node.node_name] = node

    for st in statements:
        if st.find(exp.Select) is None:
            continue
            
        if schema:
            st = _try_qualify(st, schema)
            
        cte_names = _get_cte_names(st)
        
        # Process CTEs
        for cte in st.find_all(exp.CTE):
            target_model = str(cte.alias)
            # In CTE, only its inner select matters
            inner_select = cte.this
            if not isinstance(inner_select, exp.Select):
                continue
                
            alias_to_table = _extract_alias_to_table(inner_select)
            src_nodes, edges = _resolve_select_list(inner_select, target_model, alias_to_table, cte_names, schema)
            
            # The columns projected by this CTE
            target_cols = _dedupe_preserve_order([str(e.target_column) for e in edges if e.target_column and e.target_node == target_model])
            
            # Add CTE node
            merge_nodes([GraphNodeData(
                node_name=target_model,
                node_type=LineageType.cte,
                columns=target_cols
            )])
            merge_nodes(src_nodes)
            all_edges.extend(edges)
            
        # Process Outer Query
        outer_select = st
        if isinstance(st, exp.Query) and not isinstance(st, exp.Select):
            outer_select = st.this # Might be union, etc. We simplify for model where outer is a Select.
            if not isinstance(outer_select, exp.Select):
                # Fallback to finding the first select that is not in a CTE?
                # Actually, st.find(exp.Select) could be anything.
                # Let's simplify
                pass

        if isinstance(outer_select, exp.Select):
            alias_to_table = _extract_alias_to_table(outer_select)
            src_nodes, edges = _resolve_select_list(outer_select, query_name, alias_to_table, cte_names, schema)
            
            target_cols = _dedupe_preserve_order([str(e.target_column) for e in edges if e.target_column and e.target_node == query_name])
            
            merge_nodes([GraphNodeData(
                node_name=query_name,
                node_type=LineageType.target,
                columns=target_cols
            )])
            merge_nodes(src_nodes)
            all_edges.extend(edges)

    # De-duplicate edges
    uniq_edges: set[tuple[str, str | None, str, str | None]] = set()
    deduped_edges: list[GraphEdgeData] = []
    for e in all_edges:
        key = (e.source_node, e.source_column, e.target_node, e.target_column)
        if key in uniq_edges:
            continue
        uniq_edges.add(key)
        deduped_edges.append(e)

    return ExtractedGraph(
        nodes=list(all_nodes_map.values()),
        edges=deduped_edges
    )
