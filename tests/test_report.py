"""Tests for lineage/report.py — Mermaid and HTML generation."""

import pytest

from lineage.graph import LineageGraph
from lineage.parser import ColumnMapping, LineageNode
from lineage.report import generate_mermaid, generate_html_report


def _build_simple_graph() -> LineageGraph:
    g = LineageGraph()
    g.ingest([
        LineageNode(
            target_table="staging.orders",
            source_tables=["source.raw_orders"],
            column_mappings=[ColumnMapping("order_id", "order_id")],
            transformation_type="passthrough",
            raw_sql="...",
            pipeline_name="retail_etl",
        ),
        LineageNode(
            target_table="mart.sales",
            source_tables=["staging.orders"],
            column_mappings=[ColumnMapping("revenue", "SUM(amount)")],
            transformation_type="aggregate",
            raw_sql="...",
            pipeline_name="retail_etl",
        ),
    ])
    return g


class TestMermaidGeneration:
    def test_flowchart_header(self):
        g = _build_simple_graph()
        diagram = generate_mermaid(g)
        assert "flowchart LR" in diagram

    def test_flowchart_td_direction(self):
        g = _build_simple_graph()
        diagram = generate_mermaid(g, direction="TD")
        assert "flowchart TD" in diagram

    def test_edge_present(self):
        g = _build_simple_graph()
        diagram = generate_mermaid(g)
        # At least one --> arrow
        assert "-->" in diagram

    def test_all_tables_in_diagram(self):
        g = _build_simple_graph()
        diagram = generate_mermaid(g)
        # Node IDs use underscores for dots
        assert "source_raw_orders" in diagram
        assert "staging_orders" in diagram
        assert "mart_sales" in diagram

    def test_classdefs_present(self):
        g = _build_simple_graph()
        diagram = generate_mermaid(g)
        assert "classDef source" in diagram
        assert "classDef derived" in diagram

    def test_transformation_label_in_edge(self):
        g = _build_simple_graph()
        diagram = generate_mermaid(g)
        # Transformation labels are embedded in edges
        assert "passthrough" in diagram or "aggregate" in diagram

    def test_empty_graph_produces_valid_header(self):
        g = LineageGraph()
        diagram = generate_mermaid(g)
        assert "flowchart LR" in diagram

    def test_no_duplicate_nodes(self):
        g = _build_simple_graph()
        diagram = generate_mermaid(g)
        # Count occurrences of the node definition for source.raw_orders
        count = diagram.count("source_raw_orders[")
        assert count == 1


class TestHTMLReportGeneration:
    def test_html_has_doctype(self):
        g = _build_simple_graph()
        html = generate_html_report(g)
        assert "<!DOCTYPE html>" in html

    def test_html_contains_mermaid_script(self):
        g = _build_simple_graph()
        html = generate_html_report(g)
        assert "mermaid" in html.lower()

    def test_html_contains_table_names(self):
        g = _build_simple_graph()
        html = generate_html_report(g)
        assert "staging.orders" in html
        assert "mart.sales" in html
        assert "source.raw_orders" in html

    def test_html_title_customizable(self):
        g = _build_simple_graph()
        html = generate_html_report(g, title="My Custom Report")
        assert "My Custom Report" in html

    def test_html_has_node_count(self):
        g = _build_simple_graph()
        html = generate_html_report(g)
        # 3 tables: source.raw_orders, staging.orders, mart.sales
        assert "3" in html

    def test_html_source_pill(self):
        g = _build_simple_graph()
        html = generate_html_report(g)
        assert "source" in html  # pill text

    def test_html_derived_pill(self):
        g = _build_simple_graph()
        html = generate_html_report(g)
        assert "derived" in html

    def test_empty_graph_does_not_raise(self):
        g = LineageGraph()
        html = generate_html_report(g)
        assert "<!DOCTYPE html>" in html
