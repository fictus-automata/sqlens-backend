import pytest

from app.services.lineage_service import compute_lineage


def test_simple_select_from():
    lineage = compute_lineage("SELECT col1 FROM table1")
    assert lineage["tables"] == ["table1"]
    assert lineage["columns"] == ["col1"]


def test_join_and_qualified_columns():
    sql = """
    SELECT a.col1, b.col2
    FROM table_a a
    JOIN table_b b ON a.id = b.a_id
    """
    lineage = compute_lineage(sql)
    assert lineage["tables"] == ["table_a", "table_b"]
    # Columns are unqualified; includes columns referenced beyond the SELECT projection (JOIN ... ON).
    assert lineage["columns"] == ["col1", "col2", "id", "a_id"]


def test_wildcard_star():
    lineage = compute_lineage("SELECT * FROM table1")
    assert lineage["tables"] == ["table1"]
    assert lineage["columns"] == ["*"]


def test_invalid_sql_rejected():
    with pytest.raises(ValueError):
        compute_lineage("UPDATE table1 SET x = 1")


def test_cte_unqualified_columns_not_ambiguous():
    """Unqualified columns inside a CTE body whose FROM is another CTE
    must not be marked ambiguous — they have no physical table in scope."""
    sql = """
    WITH base AS (
        SELECT order_id, total_amount, customer_id
        FROM orders
    ),
    agg AS (
        SELECT customer_id, SUM(total_amount) AS ltv
        FROM base
        GROUP BY customer_id
    )
    SELECT c.name, a.ltv
    FROM customers c
    JOIN agg a ON c.customer_id = a.customer_id
    """
    lineage = compute_lineage(sql)
    assert set(lineage["tables"]) == {"orders", "customers"}

    entry_types = {e["lineage_type"] for e in lineage["lineage_entries"]}
    # No ambiguous entries — unqualified columns inside CTE-only scopes are skipped.
    assert "ambiguous" not in entry_types


def test_cte_real_query_no_ambiguous_refs():
    """Reproduces the exact user-reported query. Columns like total_amount and
    order_id that appear inside a CTE referencing another CTE must not be
    reported as ambiguous."""
    sql = """
    WITH customer_orders AS (
        SELECT
            o.order_id,
            o.customer_id,
            o.total_amount,
            o.status,
            o.created_at AS order_date
        FROM orders o
        WHERE o.status IN ('COMPLETED', 'SHIPPED')
    ),
    customer_stats AS (
        SELECT
            customer_id,
            COUNT(order_id)      AS total_orders,
            SUM(total_amount)    AS lifetime_value,
            MAX(order_date)      AS last_order_date
        FROM customer_orders
        GROUP BY customer_id
    )
    SELECT
        c.customer_id,
        c.name,
        c.email,
        c.region,
        cs.total_orders,
        cs.lifetime_value,
        cs.last_order_date
    FROM customers c
    INNER JOIN customer_stats cs
        ON c.customer_id = cs.customer_id
    WHERE cs.lifetime_value > 1000
    ORDER BY cs.lifetime_value DESC
    """
    lineage = compute_lineage(sql)
    assert set(lineage["tables"]) == {"orders", "customers"}

    entry_types = {e["lineage_type"] for e in lineage["lineage_entries"]}
    assert "ambiguous" not in entry_types

    col_table_pairs = {
        (e["table_name"], e["column_name"])
        for e in lineage["lineage_entries"]
        if e.get("column_name")
    }
    assert ("orders", "order_id") in col_table_pairs
    assert ("orders", "total_amount") in col_table_pairs
    assert ("orders", "customer_id") in col_table_pairs
    assert ("customers", "customer_id") in col_table_pairs
    assert ("customers", "name") in col_table_pairs


def test_unqualified_column_multi_table_outer_select_still_ambiguous():
    """An unqualified column in a SELECT with two physical tables in its
    direct FROM/JOIN scope must still be marked ambiguous."""
    sql = "SELECT name FROM orders JOIN customers ON orders.customer_id = customers.id"
    lineage = compute_lineage(sql)
    entry_types = {
        e["lineage_type"]
        for e in lineage["lineage_entries"]
        if e.get("column_name") == "name"
    }
    assert "ambiguous" in entry_types
