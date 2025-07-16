"""Tests for impact/propagation_simulator.py — change impact propagation."""

from __future__ import annotations

import networkx as nx

from impact.propagation_simulator import (
    BlastRadiusReport,
    ImpactedNode,
    PropagationSimulator,
    Severity,
    _estimate_fix_hours,
)

# ---------------------------------------------------------------------------
# Fixtures: build test graphs
# ---------------------------------------------------------------------------


def _linear_graph() -> nx.DiGraph:
    """source → staging → warehouse → mart (4 nodes, 3 edges)."""
    g = nx.DiGraph()
    nodes = [
        (
            "source.orders",
            {"system": "sql", "node_type": "SourceTable", "label": "source.orders"},
        ),
        (
            "staging.orders",
            {"system": "sql", "node_type": "SinkTable", "label": "staging.orders"},
        ),
        (
            "warehouse.daily",
            {"system": "sql", "node_type": "SinkTable", "label": "warehouse.daily"},
        ),
        (
            "mart.revenue",
            {"system": "sql", "node_type": "SinkTable", "label": "mart.revenue"},
        ),
    ]
    for nid, attrs in nodes:
        g.add_node(nid, **attrs)

    edges = [
        (
            "source.orders",
            "staging.orders",
            {
                "column_mappings": [
                    {"target_col": "amount", "source_expression": "amount"}
                ],
                "system": "sql",
            },
        ),
        (
            "staging.orders",
            "warehouse.daily",
            {
                "column_mappings": [
                    {"target_col": "total", "source_expression": "SUM(amount)"}
                ],
                "system": "sql",
            },
        ),
        (
            "warehouse.daily",
            "mart.revenue",
            {
                "column_mappings": [
                    {"target_col": "revenue", "source_expression": "total"}
                ],
                "system": "sql",
            },
        ),
    ]
    for src, tgt, d in edges:
        g.add_edge(src, tgt, **d)
    return g


def _cross_system_graph() -> nx.DiGraph:
    """Multi-system graph for cross-system propagation tests."""
    g = nx.DiGraph()
    g.add_node(
        "sql::source.raw", system="sql", node_type="SourceTable", label="source.raw"
    )
    g.add_node(
        "spark::s3://processed/",
        system="spark",
        node_type="SparkDataset",
        label="s3://processed/",
    )
    g.add_node(
        "dbt::orders_fact", system="dbt", node_type="DbtModel", label="orders_fact"
    )
    g.add_node(
        "airflow::dag.task", system="airflow", node_type="AirflowTask", label="dag.task"
    )

    g.add_edge(
        "sql::source.raw",
        "spark::s3://processed/",
        system="sql",
        column_mappings=[{"target_col": "amount", "source_expression": "amount"}],
    )
    g.add_edge(
        "spark::s3://processed/",
        "dbt::orders_fact",
        system="cross",
        column_mappings=[],
        transformation_type="spark_to_dbt",
    )
    g.add_edge(
        "dbt::orders_fact",
        "airflow::dag.task",
        system="cross",
        column_mappings=[],
        transformation_type="dbt_to_airflow",
    )
    return g


def _wildcard_graph() -> nx.DiGraph:
    """Graph with a wildcard SELECT * edge."""
    g = nx.DiGraph()
    g.add_node("source.a", system="sql", node_type="SourceTable", label="source.a")
    g.add_node("staging.b", system="sql", node_type="SinkTable", label="staging.b")
    g.add_edge(
        "source.a",
        "staging.b",
        system="sql",
        column_mappings=[{"target_col": "*", "source_expression": "*"}],
    )
    return g


# ---------------------------------------------------------------------------
# Basic simulation
# ---------------------------------------------------------------------------


class TestBasicSimulation:
    def test_returns_blast_radius_report(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", "amount", "rename")
        assert isinstance(report, BlastRadiusReport)

    def test_unknown_node_returns_zero_impact(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("nonexistent.table", "col")
        assert report.total_impacted == 0

    def test_leaf_node_has_no_downstream(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("mart.revenue", "revenue")
        assert report.total_impacted == 0

    def test_source_change_propagates_to_all_downstream(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders")
        # Whole-table change → all 3 downstream nodes
        assert report.total_impacted == 3

    def test_whole_table_change_all_breaking(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", column_name=None)
        assert all(n.severity == Severity.BREAKING for n in report.impacted_nodes)

    def test_impacted_nodes_sorted_by_hop(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders")
        hops = [n.hop_distance for n in report.impacted_nodes]
        assert hops == sorted(hops)


# ---------------------------------------------------------------------------
# Severity classification
# ---------------------------------------------------------------------------


class TestSeverityClassification:
    def test_direct_column_reference_is_breaking(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", "amount", "rename")
        # staging.orders at hop 1 has a direct column mapping
        hop1 = next(n for n in report.impacted_nodes if n.hop_distance == 1)
        assert hop1.severity == Severity.BREAKING

    def test_drop_column_is_breaking(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", "amount", "drop")
        hop1 = next(n for n in report.impacted_nodes if n.hop_distance == 1)
        assert hop1.severity == Severity.BREAKING

    def test_wildcard_select_is_warning(self):
        sim = PropagationSimulator(_wildcard_graph())
        report = sim.simulate("source.a", "any_col", "rename")
        assert any(n.severity == Severity.WARNING for n in report.impacted_nodes)

    def test_add_column_is_warning_not_breaking(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", "new_col", "add")
        # "new_col" not in any mapping → WARNING or OK
        for n in report.impacted_nodes:
            assert n.severity != Severity.BREAKING


# ---------------------------------------------------------------------------
# Blast radius counts
# ---------------------------------------------------------------------------


class TestBlastRadiusCounts:
    def test_breaking_count_correct(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", "amount", "rename")
        assert report.breaking_count == sum(
            1 for n in report.impacted_nodes if n.severity == Severity.BREAKING
        )

    def test_warning_count_correct(self):
        sim = PropagationSimulator(_wildcard_graph())
        report = sim.simulate("source.a", "col", "rename")
        assert report.warning_count == sum(
            1 for n in report.impacted_nodes if n.severity == Severity.WARNING
        )

    def test_total_impacted_equals_sum_of_severities(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders")
        assert (
            report.total_impacted
            == report.breaking_count + report.warning_count + report.ok_count
        )


# ---------------------------------------------------------------------------
# Fix time estimation
# ---------------------------------------------------------------------------


class TestFixTimeEstimation:
    def test_fix_hours_positive_for_breaking(self):
        hours = _estimate_fix_hours("sql", Severity.BREAKING, hop_distance=1)
        assert hours > 0

    def test_fix_hours_zero_for_ok(self):
        hours = _estimate_fix_hours("sql", Severity.OK, hop_distance=1)
        assert hours == 0.0

    def test_spark_fix_hours_higher_than_sql(self):
        sql_h = _estimate_fix_hours("sql", Severity.BREAKING, 1)
        spark_h = _estimate_fix_hours("spark", Severity.BREAKING, 1)
        assert spark_h > sql_h

    def test_total_fix_hours_sum_of_nodes(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders")
        expected = sum(n.fix_hours for n in report.impacted_nodes)
        assert abs(report.total_fix_hours - expected) < 0.01


# ---------------------------------------------------------------------------
# Path tracking
# ---------------------------------------------------------------------------


class TestPathTracking:
    def test_path_includes_source(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders")
        for n in report.impacted_nodes:
            assert "source.orders" in n.path_from_source

    def test_path_includes_node_itself(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders")
        for n in report.impacted_nodes:
            assert n.node_id in n.path_from_source


# ---------------------------------------------------------------------------
# Cross-system propagation
# ---------------------------------------------------------------------------


class TestCrossSystemPropagation:
    def test_propagates_across_system_boundary(self):
        sim = PropagationSimulator(_cross_system_graph())
        report = sim.simulate("sql::source.raw")
        node_ids = [n.node_id for n in report.impacted_nodes]
        assert "spark::s3://processed/" in node_ids

    def test_by_system_breakdown_present(self):
        sim = PropagationSimulator(_cross_system_graph())
        report = sim.simulate("sql::source.raw")
        assert len(report.by_system) > 0

    def test_multiple_systems_in_breakdown(self):
        sim = PropagationSimulator(_cross_system_graph())
        report = sim.simulate("sql::source.raw")
        assert len(report.by_system) >= 2


# ---------------------------------------------------------------------------
# Summary and serialization
# ---------------------------------------------------------------------------


class TestReportSerialization:
    def test_summary_contains_changed_node(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", "amount")
        summary = report.summary()
        assert "source.orders" in summary

    def test_to_dict_structure(self):
        sim = PropagationSimulator(_linear_graph())
        report = sim.simulate("source.orders", "amount")
        d = report.to_dict()
        assert "changed_node" in d
        assert "impacted_nodes" in d
        assert "total_fix_hours" in d

    def test_impacted_node_to_dict(self):
        node = ImpactedNode(
            node_id="test.node",
            label="test.node",
            system="sql",
            node_type="SinkTable",
            severity=Severity.BREAKING,
            hop_distance=1,
            path_from_source=["source", "test.node"],
            reason="column renamed",
            fix_hours=1.5,
        )
        d = node.to_dict()
        assert d["severity"] == "BREAKING"
        assert d["fix_hours"] == 1.5


# ---------------------------------------------------------------------------
# Top risk nodes
# ---------------------------------------------------------------------------


class TestTopRiskNodes:
    def test_top_risk_nodes_returns_list(self):
        sim = PropagationSimulator(_linear_graph())
        result = sim.top_risk_nodes(top_n=3)
        assert isinstance(result, list)
        assert len(result) <= 3

    def test_top_risk_has_required_keys(self):
        sim = PropagationSimulator(_linear_graph())
        result = sim.top_risk_nodes(top_n=2)
        for item in result:
            assert "node_id" in item
            assert "out_degree" in item

    def test_empty_graph_returns_empty(self):
        sim = PropagationSimulator(nx.DiGraph())
        result = sim.top_risk_nodes()
        assert result == []
