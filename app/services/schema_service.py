"""schema_service.py — Converts registered table schemas into sqlglot-compatible dicts.

This module is intentionally thin: it translates ORM objects into the dict
format that ``sqlglot.optimizer.qualify_columns`` expects, keeping that
concern out of both the repository layer and the lineage extractor.
"""
from __future__ import annotations

from app.db.models import TableSchema


def build_sqlglot_schema(table_schemas: list[TableSchema]) -> dict:
    """Build a sqlglot-compatible schema dict from a list of registered schemas.

    sqlglot's ``qualify_columns`` accepts schemas in one of three nested formats
    depending on how many namespace levels are present:

    * Flat (table only):
      ``{"orders": {"id": "BIGINT", "user_id": "BIGINT"}}``
    * Two-level (schema.table):
      ``{"public": {"orders": {"id": "BIGINT"}}}``
    * Three-level (database.schema.table):
      ``{"mydb": {"public": {"orders": {"id": "BIGINT"}}}}``

    When ``data_type`` is not provided for a column, ``"TEXT"`` is used as a
    fallback — sqlglot requires *some* type to resolve expressions, but the
    exact type only matters for type-dependent disambiguation.

    Args:
        table_schemas: ORM objects with their ``columns`` relationship eagerly loaded.

    Returns:
        A nested dict ready to pass as the ``schema`` argument to
        ``sqlglot.optimizer.qualify_columns``.
    """
    schema: dict = {}

    for ts in table_schemas:
        col_mapping = {
            col.column_name: col.data_type or "TEXT"
            for col in sorted(ts.columns, key=lambda c: (c.ordinal_position or 999, c.column_name))
        }

        if ts.db_database and ts.db_schema:
            # Three-level: database.schema.table
            schema.setdefault(ts.db_database, {}).setdefault(ts.db_schema, {})[ts.table_name] = col_mapping
        elif ts.db_schema:
            # Two-level: schema.table
            schema.setdefault(ts.db_schema, {})[ts.table_name] = col_mapping
        else:
            # Flat: table
            schema[ts.table_name] = col_mapping

    return schema
