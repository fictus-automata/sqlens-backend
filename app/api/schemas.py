from datetime import datetime
from typing import Any, Dict, List
import uuid

from pydantic import BaseModel, Field


# ── Query schemas ──────────────────────────────────────────────────────────────


class CreateQueryRequest(BaseModel):
    user_id: str = Field(..., description="Client-provided user identifier")
    query_text: str = Field(..., description="SQL statement text")
    query_name: str = Field(..., description="Human-friendly label, used as target model name in graph")
    tags: Dict[str, Any] | None = Field(None, description="Flexible metadata bag")
    source_name: str | None = Field(
        None,
        description=(
            "Logical data-source name. When set, registered schemas for this source are "
            "used to fully qualify column references before lineage extraction, "
            "eliminating ambiguity when identical column names exist across tables."
        ),
    )


class CreateQueryResponse(BaseModel):
    id: uuid.UUID
    user_id: str
    created_at: datetime
    lineage_status: str


class QueryListItem(BaseModel):
    id: uuid.UUID
    user_id: str
    query_text: str
    query_name: str | None = None
    tags: Dict[str, Any] | None = None
    source_name: str | None = None
    created_at: datetime
    updated_at: datetime
    lineage_status: str


class QueryListResponse(BaseModel):
    items: List[QueryListItem]
    total: int = Field(..., description="Total number of matching queries (ignoring limit/offset)")
    limit: int
    offset: int


class QueryResponse(BaseModel):
    id: uuid.UUID
    user_id: str
    query_text: str
    query_name: str | None
    tags: Dict[str, Any] | None
    source_name: str | None = None
    created_at: datetime
    updated_at: datetime
    lineage_status: str
    parse_error: str | None = None


# ── Lineage response schemas ───────────────────────────────────────────────────


class ColumnRef(BaseModel):
    """A fully-attributed column reference: we know exactly which table it belongs to."""

    table: str
    column: str
    lineage_type: str = Field(default="source", description="'source', 'target', etc.")


class AmbiguousRef(BaseModel):
    """A column reference that could not be attributed to a specific table.

    Occurs when a column is unqualified in a multi-table query and no schema
    was registered to resolve the ambiguity. The ``candidate_tables`` list
    contains all physical tables that were in scope for the query.
    """

    column: str
    candidate_tables: List[str]


class LineageResponse(BaseModel):
    """Lineage for a stored query.

    ``column_refs`` replaces the old ``column_lineage`` dict, which collapsed
    distinct ``(table, column)`` pairs under a bare column name key and lost
    information when two tables shared the same column name.
    """

    tables: List[str]
    column_refs: List[ColumnRef]
    ambiguous_refs: List[AmbiguousRef] = Field(
        default_factory=list,
        description=(
            "Columns that could not be attributed to a specific table. "
            "Register a schema via POST /schemas to resolve these."
        ),
    )


# ── Inverse lineage schemas ────────────────────────────────────────────────────


class TableListResponse(BaseModel):
    """All distinct table names referenced across every stored query."""

    tables: List[str]


class QuerySummary(BaseModel):
    """Lightweight query representation used in inverse-lineage responses."""

    id: uuid.UUID
    user_id: str
    query_name: str | None = None
    created_at: datetime
    lineage_status: str


class QueriesByTableResponse(BaseModel):
    """All queries whose lineage references a specific table."""

    table_name: str
    queries: List[QuerySummary]


# ── Schema Registry request/response schemas ───────────────────────────────────


class ColumnDefinition(BaseModel):
    """Column metadata for schema registration."""

    column_name: str = Field(..., description="Column name as it appears in SQL")
    data_type: str | None = Field(
        None,
        description=(
            "SQL data type (e.g. 'BIGINT', 'TEXT', 'TIMESTAMP'). Used by sqlglot's "
            "qualify_columns to resolve type-dependent expression ambiguities. "
            "Omit if unknown — the column will still be used for table attribution."
        ),
    )
    is_nullable: bool | None = None
    ordinal_position: int | None = Field(None, description="1-based column order in the table")


class RegisterSchemaRequest(BaseModel):
    source_name: str = Field(
        ...,
        description=(
            "Logical data-source identifier (e.g. 'production_db'). Namespaces schemas "
            "so that 'orders' in one source does not collide with 'orders' in another."
        ),
    )
    db_database: str | None = Field(None, description="Optional database name for 3-part identifiers")
    db_schema: str | None = Field(None, description="Optional schema name (e.g. 'public', 'dbo')")
    table_name: str = Field(..., description="Unqualified table name")
    dialect: str = Field("postgres", description="SQL dialect (passed to sqlglot)")
    columns: List[ColumnDefinition] = Field(..., min_length=1)


class RegisterSchemaResponse(BaseModel):
    id: uuid.UUID
    source_name: str
    db_database: str | None
    db_schema: str | None
    table_name: str
    dialect: str
    column_count: int
    created_at: datetime
    updated_at: datetime


class SchemaColumnDetail(BaseModel):
    column_name: str
    data_type: str | None = None
    is_nullable: bool | None = None
    ordinal_position: int | None = None


class SchemaDetail(RegisterSchemaResponse):
    """Full table schema including column definitions."""

    columns: List[SchemaColumnDetail]


class SchemaListResponse(BaseModel):
    items: List[RegisterSchemaResponse]
    total: int


class SourceListResponse(BaseModel):
    """All distinct source_name values registered in the schema registry."""

    sources: List[str]


# ── Graph response schemas ───────────────────────────────────────────────────


class GraphColumnInfo(BaseModel):
    name: str
    data_type: str | None = None


class GraphNodeResponse(BaseModel):
    id: str  # "{node_type}::{node_name}"
    node_name: str
    node_type: str  # "source" | "cte" | "target"
    columns: List[GraphColumnInfo]


class GraphEdgeResponse(BaseModel):
    id: str  # "{source_node_id}::{source_col}->{target_node_id}::{target_col}"
    source_node_id: str
    source_column: str | None
    target_node_id: str
    target_column: str | None


class GraphResponse(BaseModel):
    nodes: List[GraphNodeResponse]
    edges: List[GraphEdgeResponse]
    query_ids: List[uuid.UUID]

