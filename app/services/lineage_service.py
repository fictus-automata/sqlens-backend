from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, List, TypedDict

from sqlglot import exp, parse
from sqlglot.errors import ParseError

from app.core.logging import get_logger

log = get_logger(__name__)

try:
    from sqlglot.optimizer.qualify_columns import qualify_columns as _sqlglot_qualify_columns

    _QUALIFY_AVAILABLE = True
except ImportError:  # pragma: no cover
    _QUALIFY_AVAILABLE = False


def _dedupe_preserve_order(items: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _get_cte_names(expression: exp.Expression) -> set[str]:
    names: set[str] = set()
    for cte in expression.find_all(exp.CTE):
        name = cte.alias_or_name
        if name:
            names.add(str(name))
    return names


def _extract_alias_to_table(expression: exp.Expression) -> dict[str, str]:
    """Build alias -> real table mapping for physical tables.

    Example: ``FROM table_a a`` => ``{"a": "table_a"}``
    """
    mapping: dict[str, str] = {}
    for t in expression.find_all(exp.Table):
        real = t.name
        if not real:
            continue
        alias = t.alias
        if alias:
            alias_name = alias.name if hasattr(alias, "name") else str(alias)
            if alias_name and alias_name != real:
                mapping[alias_name] = real
    return mapping


def _extract_projection_has_star(expression: exp.Expression) -> bool:
    # sqlglot has Star nodes for `*` and for `t.*`.
    return any(expression.find_all(exp.Star))


def _get_scope_tables_for_col(col: exp.Column, cte_names: set[str]) -> list[str]:
    """Return the physical table names directly in scope for *col*.

    Walks up the AST from *col* to find its nearest enclosing ``exp.Select``,
    then inspects **only** that SELECT's direct FROM / JOIN expressions —
    deliberately ignoring tables from outer queries, sibling CTEs, or nested
    sub-selects.  CTE names are excluded so that references to CTE outputs are
    not mistaken for physical tables.

    Note: sqlglot uses the key ``from_`` (not ``from``) on ``exp.Select.args``
    to avoid shadowing the Python built-in.  Using ``select.args`` rather than
    ``find_all`` prevents accidentally picking up tables from nested subqueries.
    """
    # Walk up to the nearest enclosing SELECT.
    node: exp.Expression | None = col.parent
    while node is not None and not isinstance(node, exp.Select):
        node = node.parent
    if node is None:
        return []

    select: exp.Select = node  # type: ignore[assignment]
    scope_tables: list[str] = []

    # Direct FROM clause only — "from_" is the sqlglot key (not "from").
    from_ = select.args.get("from_")
    if from_:
        for t in from_.find_all(exp.Table):
            real = t.name
            if real and str(real) not in cte_names:
                scope_tables.append(str(real))

    # Direct JOINs on this SELECT only.
    for join in select.args.get("joins") or []:
        for t in join.find_all(exp.Table):
            real = t.name
            if real and str(real) not in cte_names:
                scope_tables.append(str(real))

    return _dedupe_preserve_order(scope_tables)


class LineageEntry(TypedDict, total=False):
    table_name: str
    column_name: str | None
    lineage_type: str  # "source" | "target" | "cte" | "ambiguous"


class ExtractedLineage(TypedDict):
    lineage_entries: List[LineageEntry]
    tables: List[str]
    columns: List[str]


def _try_qualify(statement: exp.Expression, schema: dict) -> exp.Expression:
    """Attempt to fully qualify all column references in *statement* using *schema*.

    On any failure (e.g. ambiguous column, schema mismatch) the original
    unmodified statement is returned so the caller can fall back to the v0
    heuristic path.
    """
    if not _QUALIFY_AVAILABLE or not schema:
        return statement
    try:
        return _sqlglot_qualify_columns(statement, schema=schema)
    except Exception as exc:
        log.debug("qualify_columns_failed", reason=str(exc))
        return statement


def _extract_lineage_from_statement(
    statement: exp.Expression,
    dialect: str,
    schema: dict | None = None,
) -> ExtractedLineage:
    """Extract tables and column (table, column) pairs from a single SELECT statement.

    When *schema* is provided, ``sqlglot.optimizer.qualify_columns`` is run
    first so that every ``exp.Column`` node carries an unambiguous table
    qualifier.  Without a schema, unqualified columns in multi-table queries
    are stored as ``lineage_type="ambiguous"`` entries (one row per candidate
    table) rather than being silently misattributed.
    """
    # Optionally qualify columns using the registered schema.
    if schema:
        statement = _try_qualify(statement, schema)

    cte_names = _get_cte_names(statement)
    alias_to_table = _extract_alias_to_table(statement)

    physical_tables: List[str] = []
    for t in statement.find_all(exp.Table):
        real = t.name
        if real and real not in cte_names:
            physical_tables.append(str(real))
    physical_tables = _dedupe_preserve_order(physical_tables)

    has_star = _extract_projection_has_star(statement)

    entries: list[LineageEntry] = []
    # Always emit table-level refs for physical tables.
    for tbl in physical_tables:
        entries.append({"table_name": tbl, "column_name": None, "lineage_type": "source"})

    columns: List[str] = []

    if has_star:
        columns = ["*"] if physical_tables else []
        for tbl in physical_tables:
            entries.append({"table_name": tbl, "column_name": "*", "lineage_type": "source"})
        return {"lineage_entries": entries, "tables": physical_tables, "columns": columns}

    col_names: List[str] = []
    seen_cols: set[tuple[str, str, str]] = set()

    for col in statement.find_all(exp.Column):
        if not col.name:
            continue

        col_name = str(col.name)
        owning_qualifier = col.table

        if owning_qualifier:
            qualifier = str(owning_qualifier)
            table_name = alias_to_table.get(qualifier, qualifier)
            if not table_name or table_name in cte_names:
                continue
            key = (table_name, col_name, "source")
            if key in seen_cols:
                continue
            seen_cols.add(key)
            entries.append({"table_name": table_name, "column_name": col_name, "lineage_type": "source"})
            col_names.append(col_name)

        else:
            # Unqualified column — use scope-aware tables (only the physical
            # tables in the immediate FROM/JOIN of the enclosing SELECT).
            # This correctly handles CTEs: a column inside a CTE body whose
            # only source is another CTE returns an empty scope, so it is
            # skipped rather than being wrongly labelled as ambiguous.
            scope_tables = _get_scope_tables_for_col(col, cte_names)

            if len(scope_tables) == 0:
                # Column is inside a CTE-only scope; no physical attribution.
                continue

            elif len(scope_tables) == 1:
                table_name = scope_tables[0]
                key = (table_name, col_name, "source")
                if key not in seen_cols:
                    seen_cols.add(key)
                    entries.append({"table_name": table_name, "column_name": col_name, "lineage_type": "source"})
                    col_names.append(col_name)

            else:
                # Genuinely ambiguous: multiple physical tables in this scope.
                any_new = False
                for candidate in scope_tables:
                    key = (candidate, col_name, "ambiguous")
                    if key not in seen_cols:
                        seen_cols.add(key)
                        entries.append(
                            {"table_name": candidate, "column_name": col_name, "lineage_type": "ambiguous"}
                        )
                        any_new = True
                if any_new:
                    col_names.append(col_name)

    columns = _dedupe_preserve_order(col_names)
    return {"lineage_entries": entries, "tables": physical_tables, "columns": columns}


def _run_extraction(sql: str, dialect: str, schema: dict | None) -> Dict[str, Any]:
    """Core extraction logic shared by both the cached and schema-aware paths."""
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

    if not any(st.find(exp.Select) is not None for st in statements):
        raise ValueError("Unsupported SQL: expected SELECT statement")

    all_tables: List[str] = []
    all_columns: List[str] = []
    all_entries: list[LineageEntry] = []

    for st in statements:
        if st.find(exp.Select) is None:
            continue
        extracted = _extract_lineage_from_statement(st, dialect=dialect, schema=schema)
        all_tables.extend(extracted["tables"])
        all_columns.extend(extracted["columns"])
        all_entries.extend(extracted["lineage_entries"])

    all_tables = _dedupe_preserve_order(all_tables)
    all_columns = _dedupe_preserve_order(all_columns)

    if not all_tables:
        raise ValueError("Could not extract referenced tables from SQL")

    # De-duplicate lineage entries.
    uniq: set[tuple[str, str | None, str]] = set()
    deduped_entries: list[LineageEntry] = []
    for e in all_entries:
        key = (e["table_name"], e.get("column_name"), e["lineage_type"])
        if key in uniq:
            continue
        uniq.add(key)
        deduped_entries.append(e)

    return {"tables": all_tables, "columns": all_columns, "lineage_entries": deduped_entries}


@lru_cache(maxsize=10_000)
def _cached_compute_lineage(sql: str, dialect: str) -> Dict[str, Any]:
    """Schema-free lineage extraction, cached by (sql, dialect).

    The cache is process-local and never invalidated — safe because extraction
    is deterministic for a given SQL string.  In production this benefits BI
    tools that repeatedly submit the same templated queries.
    """
    return _run_extraction(sql, dialect, schema=None)


def compute_lineage(sql: str, dialect: str = "postgres", schema: dict | None = None) -> Dict[str, Any]:
    """Extract lineage from *sql* using SQLGlot AST traversal.

    Only SELECT statements are supported in v0 — UPDATE/INSERT/DDL raises
    ``ValueError`` so the caller records ``lineage_status=failed`` without
    blocking query ingest.

    Args:
        sql: Raw SQL string (will be stripped).
        dialect: sqlglot dialect name used for parsing (default: ``postgres``).
        schema: Optional sqlglot-compatible schema dict as produced by
            :func:`app.services.schema_service.build_sqlglot_schema`.
            When provided, ``qualify_columns`` is run before extraction so that
            every column reference is unambiguously attributed to a table.
            Schema-aware calls are **not cached** because the schema dict is
            mutable; no-schema calls are cached by ``(sql, dialect)``.

    Returns:
        A dict with three keys:

        - ``tables``: deduplicated list of physical table names.
        - ``columns``: deduplicated list of referenced column names.
        - ``lineage_entries``: flat list of
          ``{table_name, column_name, lineage_type}`` records ready for DB
          persistence. Entries with ``lineage_type="ambiguous"`` represent
          columns that could not be attributed without schema context.

    Raises:
        ValueError: If *sql* is empty, cannot be parsed, or contains no
            SELECT statement.
    """
    if schema is None:
        return _cached_compute_lineage(sql, dialect)
    # Schema-aware path: bypass cache (schema dicts are not hashable).



    return _run_extraction(sql, dialect, schema=schema)
