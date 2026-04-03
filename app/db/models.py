from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON
from sqlalchemy import UUID as SAUUID


class Base(DeclarativeBase):
    pass


class LineageStatus(str, enum.Enum):
    pending = "pending"
    completed = "completed"
    failed = "failed"
    # v1: add `stale = "stale"` once schema-update + re-extraction are supported.


class LineageType(str, enum.Enum):
    source = "source"
    target = "target"
    cte = "cte"
    subquery = "subquery"
    ambiguous = "ambiguous"  # column could not be attributed to a specific table


class SQLQuery(Base):
    __tablename__ = "sql_queries"

    id: Mapped[uuid.UUID] = mapped_column(SAUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[str] = mapped_column(String(length=255), nullable=False, index=True)
    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    query_name: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    tags: Mapped[dict | None] = mapped_column(JSON().with_variant(JSONB(), "postgresql"), nullable=True)
    # Soft reference to table_schemas.source_name — no FK so schema registration is optional.
    source_name: Mapped[str | None] = mapped_column(String(length=255), nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    lineage_status: Mapped[LineageStatus] = mapped_column(
        Enum(LineageStatus, name="lineage_status"),
        nullable=False,
        default=LineageStatus.pending,
        server_default=LineageStatus.pending.value,
    )
    parse_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    lineage_rows: Mapped[list["QueryLineage"]] = relationship(
        "QueryLineage",
        back_populates="query",
        cascade="all, delete-orphan",
    )

    __table_args__ = (Index("ix_sql_queries_user_created_at", "user_id", "created_at"),)


class QueryLineage(Base):
    __tablename__ = "query_lineage"

    id: Mapped[uuid.UUID] = mapped_column(SAUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    query_id: Mapped[uuid.UUID] = mapped_column(
        SAUUID(as_uuid=True),
        ForeignKey("sql_queries.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    table_name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    # column_name is nullable for table-level refs.
    # For lineage_type="ambiguous", table_name holds one of the candidate tables;
    # multiple rows with the same column_name represent the full candidate set.
    column_name: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    lineage_type: Mapped[LineageType] = mapped_column(
        Enum(LineageType, name="lineage_type"),
        nullable=False,
        default=LineageType.source,
        server_default=LineageType.source.value,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    query: Mapped["SQLQuery"] = relationship("SQLQuery", back_populates="lineage_rows")

    __table_args__ = (Index("ix_query_lineage_table_name", "table_name"),)


# ── Schema Registry models ─────────────────────────────────────────────────────


class TableSchema(Base):
    """Registered schema for a logical table within a named data source.

    ``source_name`` is the namespace key — the same table name can exist in
    multiple sources without collision (e.g. ``orders`` in ``production_db``
    vs ``orders`` in ``analytics_warehouse``).
    """

    __tablename__ = "table_schemas"

    id: Mapped[uuid.UUID] = mapped_column(SAUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_name: Mapped[str] = mapped_column(String(length=255), nullable=False, index=True)
    # Optional namespace levels for 2-part (schema.table) or 3-part (db.schema.table) names.
    db_database: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    db_schema: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    table_name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    dialect: Mapped[str] = mapped_column(
        String(length=50), nullable=False, default="postgres", server_default="postgres"
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    columns: Mapped[list["SchemaColumn"]] = relationship(
        "SchemaColumn",
        back_populates="table_schema",
        cascade="all, delete-orphan",
        order_by="SchemaColumn.ordinal_position",
    )

    __table_args__ = (
        # Unique per (source, optional db, optional schema, table).
        UniqueConstraint("source_name", "db_database", "db_schema", "table_name", name="uq_table_schema"),
    )


class SchemaColumn(Base):
    """A single column definition within a registered :class:`TableSchema`."""

    __tablename__ = "schema_columns"

    id: Mapped[uuid.UUID] = mapped_column(SAUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    table_schema_id: Mapped[uuid.UUID] = mapped_column(
        SAUUID(as_uuid=True),
        ForeignKey("table_schemas.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    column_name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    data_type: Mapped[str | None] = mapped_column(String(length=100), nullable=True)
    is_nullable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    ordinal_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    table_schema: Mapped["TableSchema"] = relationship("TableSchema", back_populates="columns")

    __table_args__ = (UniqueConstraint("table_schema_id", "column_name", name="uq_schema_column"),)


# ── Graph models ──────────────────────────────────────────────────────────────


class GraphNode(Base):
    """A node in the query lineage graph (either a source table or a target/cte model)."""

    __tablename__ = "graph_nodes"

    id: Mapped[uuid.UUID] = mapped_column(SAUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    query_id: Mapped[uuid.UUID] = mapped_column(
        SAUUID(as_uuid=True),
        ForeignKey("sql_queries.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    node_name: Mapped[str] = mapped_column(String(length=255), nullable=False)
    # Reusing LineageType enum: source (table), cte (CTE), target (query result)
    node_type: Mapped[LineageType] = mapped_column(
        Enum(LineageType, name="lineage_type", create_type=False),
        nullable=False,
    )

    __table_args__ = (UniqueConstraint("query_id", "node_name", name="uq_graph_node_query"),)


class GraphEdge(Base):
    """A directed edge in the query lineage graph, representing column-level or table-level flow."""

    __tablename__ = "graph_edges"

    id: Mapped[uuid.UUID] = mapped_column(SAUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    query_id: Mapped[uuid.UUID] = mapped_column(
        SAUUID(as_uuid=True),
        ForeignKey("sql_queries.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_node: Mapped[str] = mapped_column(String(length=255), nullable=False)
    source_column: Mapped[str | None] = mapped_column(String(length=255), nullable=True)
    target_node: Mapped[str] = mapped_column(String(length=255), nullable=False)
    target_column: Mapped[str | None] = mapped_column(String(length=255), nullable=True)

    __table_args__ = (
        Index("ix_graph_edges_target_node", "target_node"),
        Index("ix_graph_edges_source_node", "source_node"),
    )

