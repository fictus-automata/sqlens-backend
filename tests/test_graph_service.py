from typing import Any

import pytest
from sqlglot.errors import ParseError

from app.db.models import LineageType
from app.services.graph_service import extract_graph

def test_extract_graph_simple_select():
    sql = "SELECT id, name FROM users"
    graph = extract_graph(sql, query_name="user_query")
    
    assert len(graph.nodes) == 2
    source_node = next(n for n in graph.nodes if n.node_type == LineageType.source)
    target_node = next(n for n in graph.nodes if n.node_type == LineageType.target)
    
    assert source_node.node_name == "users"
    assert "id" in source_node.columns
    assert "name" in source_node.columns
    
    assert target_node.node_name == "user_query"
    assert "id" in target_node.columns
    assert "name" in target_node.columns
    
    assert len(graph.edges) == 2
    assert any(e.source_node == "users" and e.source_column == "id" and e.target_node == "user_query" and e.target_column == "id" for e in graph.edges)

def test_extract_graph_with_cte():
    sql = """
    WITH active_users AS (
        SELECT id, email FROM users WHERE status = 'active'
    )
    SELECT id, email FROM active_users
    """
    graph = extract_graph(sql, query_name="active_users_query")
    
    assert len(graph.nodes) == 3
    node_names = {n.node_name for n in graph.nodes}
    assert node_names == {"users", "active_users", "active_users_query"}
    
    assert len(graph.edges) == 4
    # users -> active_users
    assert any(e.source_node == "users" and e.source_column == "id" and e.target_node == "active_users" and e.target_column == "id" for e in graph.edges)
    # active_users -> active_users_query
    assert any(e.source_node == "active_users" and e.source_column == "email" and e.target_node == "active_users_query" and e.target_column == "email" for e in graph.edges)

def test_extract_graph_aggregate_and_alias():
    sql = "SELECT SUM(amount) AS total_sales FROM orders"
    graph = extract_graph(sql, query_name="sales_query")
    
    assert len(graph.nodes) == 2
    assert len(graph.edges) == 1
    
    edge = graph.edges[0]
    assert edge.source_node == "orders"
    assert edge.source_column == "amount"
    assert edge.target_node == "sales_query"
    assert edge.target_column == "total_sales"

def test_extract_graph_select_star_no_schema():
    sql = "SELECT * FROM products"
    graph = extract_graph(sql, query_name="product_query")
    
    assert len(graph.edges) == 1
    edge = graph.edges[0]
    assert edge.source_node == "products"
    assert edge.source_column == "*"
    assert edge.target_node == "product_query"
    assert edge.target_column == "*"

def test_extract_graph_select_star_with_schema():
    schema = {"products": {"id": "int", "name": "varchar"}}
    sql = "SELECT * FROM products"
    graph = extract_graph(sql, query_name="product_query", schema=schema)
    
    assert len(graph.edges) == 2
    assert any(e.source_column == "id" for e in graph.edges)
    assert any(e.source_column == "name" for e in graph.edges)
    assert not any(e.source_column == "*" for e in graph.edges)
