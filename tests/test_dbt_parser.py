"""Tests for parsers/dbt_lineage_parser.py — dbt model lineage extraction."""

from __future__ import annotations

import textwrap

from parsers.dbt_lineage_parser import build_dbt_dependency_graph, parse_dbt_model

# ---------------------------------------------------------------------------
# ref() and source() extraction
# ---------------------------------------------------------------------------


class TestDbtRefExtraction:
    def test_single_ref(self):
        sql = "SELECT * FROM {{ ref('stg_orders') }}"
        node = parse_dbt_model(sql, "orders_fact")
        assert "stg_orders" in node.ref_deps

    def test_multiple_refs(self):
        sql = textwrap.dedent("""\
            SELECT o.order_id, c.name
            FROM {{ ref('stg_orders') }} o
            JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.id
        """)
        node = parse_dbt_model(sql, "orders_enriched")
        assert "stg_orders" in node.ref_deps
        assert "stg_customers" in node.ref_deps

    def test_ref_with_double_quotes(self):
        sql = 'SELECT * FROM {{ ref("stg_products") }}'
        node = parse_dbt_model(sql, "products_fact")
        assert "stg_products" in node.ref_deps

    def test_source_extraction(self):
        sql = "SELECT * FROM {{ source('raw', 'orders') }}"
        node = parse_dbt_model(sql, "stg_orders")
        assert ("raw", "orders") in node.source_deps

    def test_multiple_sources(self):
        sql = textwrap.dedent("""\
            SELECT o.order_id, c.name
            FROM {{ source('raw', 'orders') }} o
            JOIN {{ source('raw', 'customers') }} c ON o.customer_id = c.id
        """)
        node = parse_dbt_model(sql, "enriched")
        assert ("raw", "orders") in node.source_deps
        assert ("raw", "customers") in node.source_deps

    def test_no_deps_returns_empty(self):
        sql = "SELECT 1 AS constant_col"
        node = parse_dbt_model(sql, "const_model")
        assert node.ref_deps == []
        assert node.source_deps == []


# ---------------------------------------------------------------------------
# Warehouse table mapping
# ---------------------------------------------------------------------------


class TestWarehouseTableMapping:
    def test_default_mapping(self):
        sql = "SELECT 1"
        node = parse_dbt_model(sql, "my_model")
        assert node.warehouse_table == "analytics.public.my_model"

    def test_custom_database_schema(self):
        sql = "SELECT 1"
        node = parse_dbt_model(
            sql, "fact_orders", database="prod_db", schema="reporting"
        )
        assert node.warehouse_table == "prod_db.reporting.fact_orders"

    def test_model_name_preserved(self):
        sql = "SELECT 1"
        node = parse_dbt_model(sql, "regional_revenue")
        assert node.model_name == "regional_revenue"


# ---------------------------------------------------------------------------
# Materialization
# ---------------------------------------------------------------------------


class TestMaterialization:
    def test_table_materialization(self):
        sql = "{{ config(materialized='table') }}\nSELECT 1"
        node = parse_dbt_model(sql, "my_model")
        assert node.materialization == "table"

    def test_view_materialization(self):
        sql = "{{ config(materialized='view') }}\nSELECT 1"
        node = parse_dbt_model(sql, "my_model")
        assert node.materialization == "view"

    def test_incremental_materialization(self):
        sql = "{{ config(materialized='incremental') }}\nSELECT 1"
        node = parse_dbt_model(sql, "my_model")
        assert node.materialization == "incremental"

    def test_default_is_view(self):
        sql = "SELECT 1"
        node = parse_dbt_model(sql, "no_config")
        assert node.materialization == "view"


# ---------------------------------------------------------------------------
# Column-level lineage
# ---------------------------------------------------------------------------


class TestColumnLineage:
    def test_alias_extracted(self):
        sql = textwrap.dedent("""\
            {{ config(materialized='table') }}
            SELECT
                customer_id,
                price * qty AS amount,
                region AS customer_region
            FROM {{ ref('stg_orders') }}
        """)
        node = parse_dbt_model(sql, "orders_fact")
        target_cols = [c.target_col for c in node.column_lineage]
        assert "amount" in target_cols
        assert "customer_region" in target_cols

    def test_passthrough_col(self):
        sql = "SELECT order_id, status FROM {{ ref('raw_orders') }}"
        node = parse_dbt_model(sql, "stg_orders")
        target_cols = [c.target_col for c in node.column_lineage]
        assert "order_id" in target_cols

    def test_no_columns_on_star(self):
        sql = "SELECT * FROM {{ ref('raw') }}"
        node = parse_dbt_model(sql, "pass_through")
        # wildcard columns excluded
        assert not any(c.target_col == "*" for c in node.column_lineage)

    def test_empty_sql_no_lineage(self):
        sql = "{{ config(materialized='view') }}"
        node = parse_dbt_model(sql, "empty_model")
        assert node.column_lineage == []


# ---------------------------------------------------------------------------
# all_upstream property
# ---------------------------------------------------------------------------


class TestAllUpstream:
    def test_all_upstream_combines_refs_and_sources(self):
        sql = textwrap.dedent("""\
            SELECT o.id, s.name
            FROM {{ ref('stg_orders') }} o
            JOIN {{ source('crm', 'contacts') }} s ON o.customer_id = s.id
        """)
        node = parse_dbt_model(sql, "enriched")
        upstream = node.all_upstream
        assert "stg_orders" in upstream
        assert "crm.contacts" in upstream


# ---------------------------------------------------------------------------
# Dependency graph builder
# ---------------------------------------------------------------------------


class TestDependencyGraph:
    def test_build_graph_from_nodes(self):
        node_a = parse_dbt_model("SELECT * FROM {{ ref('raw_orders') }}", "stg_orders")
        node_b = parse_dbt_model("SELECT * FROM {{ ref('stg_orders') }}", "orders_fact")
        graph = build_dbt_dependency_graph([node_a, node_b])
        assert "orders_fact" in graph
        assert "stg_orders" in graph["orders_fact"]

    def test_empty_nodes_returns_empty_graph(self):
        graph = build_dbt_dependency_graph([])
        assert graph == {}

    def test_model_with_no_deps_has_empty_list(self):
        node = parse_dbt_model("SELECT 1", "seed_model")
        graph = build_dbt_dependency_graph([node])
        assert graph["seed_model"] == []
