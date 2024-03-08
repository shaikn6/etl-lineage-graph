"""Tests for lineage/parser.py — SQL parsing and LineageNode extraction."""

import pytest

from lineage.parser import parse_sql, LineageNode, ColumnMapping


# ---------------------------------------------------------------------------
# INSERT INTO
# ---------------------------------------------------------------------------

class TestInsertIntoBasic:
    def test_simple_insert_select(self):
        sql = """
        INSERT INTO staging.orders (order_id, amount)
        SELECT o.order_id, o.amount FROM source.raw_orders o
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        node = nodes[0]
        assert node.target_table == "orders"
        assert "raw_orders" in node.source_tables

    def test_insert_with_where_filter(self):
        sql = """
        INSERT INTO clean.events (event_id, ts)
        SELECT e.event_id, e.created_at AS ts
        FROM raw.events e
        WHERE e.event_type != 'bot'
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        assert nodes[0].transformation_type == "filter"

    def test_insert_with_join(self):
        sql = """
        INSERT INTO staging.enriched (id, name, category)
        SELECT o.id, c.name, p.category
        FROM staging.orders o
        JOIN source.customers c ON o.customer_id = c.customer_id
        JOIN source.products  p ON o.product_id  = p.product_id
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        node = nodes[0]
        sources = node.source_tables
        assert any("orders" in t for t in sources)
        assert any("customers" in t for t in sources)
        assert any("products" in t for t in sources)
        assert node.transformation_type == "join"

    def test_insert_aggregate(self):
        sql = """
        INSERT INTO warehouse.daily_summary (dt, total_revenue)
        SELECT order_date, SUM(amount) AS total_revenue
        FROM staging.orders
        GROUP BY order_date
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        assert nodes[0].transformation_type == "aggregate"


# ---------------------------------------------------------------------------
# CREATE TABLE AS
# ---------------------------------------------------------------------------

class TestCreateTableAs:
    def test_create_table_as_select(self):
        sql = """
        CREATE TABLE mart.sales AS
        SELECT * FROM warehouse.daily_summary
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        assert nodes[0].target_table == "sales"
        assert any("daily_summary" in t for t in nodes[0].source_tables)

    def test_create_table_as_with_alias(self):
        sql = """
        CREATE TABLE reporting.kpis AS
        SELECT s.total_revenue, s.region
        FROM warehouse.daily_summary s
        WHERE s.region IS NOT NULL
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        assert nodes[0].target_table == "kpis"


# ---------------------------------------------------------------------------
# Column mappings
# ---------------------------------------------------------------------------

class TestColumnMappings:
    def test_alias_is_target_col(self):
        sql = """
        INSERT INTO staging.dim_customers (full_name)
        SELECT TRIM(first_name || ' ' || last_name) AS full_name
        FROM source.customers
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        mappings = nodes[0].column_mappings
        target_cols = [m.target_col for m in mappings]
        assert "full_name" in target_cols

    def test_passthrough_col_no_alias(self):
        sql = """
        INSERT INTO staging.orders (order_id)
        SELECT order_id FROM source.orders
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        mappings = nodes[0].column_mappings
        assert any(m.target_col == "order_id" for m in mappings)


# ---------------------------------------------------------------------------
# CTEs
# ---------------------------------------------------------------------------

class TestCTEs:
    def test_cte_not_in_source_tables(self):
        sql = """
        INSERT INTO mart.sales_performance (report_date, revenue)
        WITH rolling AS (
            SELECT order_date, SUM(revenue) AS revenue
            FROM warehouse.daily_summary
            GROUP BY order_date
        )
        SELECT order_date AS report_date, revenue FROM rolling
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        # CTE name 'rolling' should NOT appear as a source table
        assert "rolling" not in nodes[0].source_tables

    def test_cte_real_source_table_present(self):
        sql = """
        INSERT INTO mart.final (dt, total)
        WITH base AS (
            SELECT order_date AS dt, SUM(amount) AS total
            FROM source.transactions
            GROUP BY order_date
        )
        SELECT dt, total FROM base
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 1
        sources = nodes[0].source_tables
        assert "base" not in sources


# ---------------------------------------------------------------------------
# Multi-statement
# ---------------------------------------------------------------------------

class TestMultiStatement:
    def test_two_inserts_parsed(self):
        sql = """
        INSERT INTO staging.a (id) SELECT id FROM source.x;
        INSERT INTO staging.b (id) SELECT id FROM source.y;
        """
        nodes = parse_sql(sql)
        assert len(nodes) == 2
        targets = {n.target_table for n in nodes}
        assert "a" in targets
        assert "b" in targets

    def test_empty_string_returns_empty(self):
        nodes = parse_sql("")
        assert nodes == []

    def test_comments_only_returns_empty(self):
        nodes = parse_sql("-- just a comment\n/* another */")
        assert nodes == []


# ---------------------------------------------------------------------------
# Transformation types
# ---------------------------------------------------------------------------

class TestTransformationTypes:
    def test_passthrough_detected(self):
        sql = """
        INSERT INTO staging.mirror (id, val)
        SELECT id, val FROM source.data
        """
        nodes = parse_sql(sql)
        assert nodes[0].transformation_type == "passthrough"

    def test_filter_detected(self):
        sql = """
        INSERT INTO staging.active (id)
        SELECT id FROM source.data WHERE is_active = TRUE
        """
        nodes = parse_sql(sql)
        assert nodes[0].transformation_type == "filter"

    def test_aggregate_takes_priority_over_filter(self):
        sql = """
        INSERT INTO warehouse.summary (dt, cnt)
        SELECT dt, COUNT(*) AS cnt FROM source.data
        WHERE dt > '2021-01-01'
        GROUP BY dt
        """
        nodes = parse_sql(sql)
        assert nodes[0].transformation_type == "aggregate"


# ---------------------------------------------------------------------------
# Pipeline name propagation
# ---------------------------------------------------------------------------

class TestPipelineName:
    def test_pipeline_name_set(self):
        sql = "INSERT INTO staging.x (id) SELECT id FROM source.raw"
        nodes = parse_sql(sql, pipeline_name="daily_etl")
        assert nodes[0].pipeline_name == "daily_etl"

    def test_pipeline_name_none_by_default(self):
        sql = "INSERT INTO staging.x (id) SELECT id FROM source.raw"
        nodes = parse_sql(sql)
        assert nodes[0].pipeline_name is None
