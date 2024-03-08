"""Tests for lineage/graph.py — LineageGraph construction and queries."""

import pytest

from lineage.graph import LineageGraph
from lineage.parser import ColumnMapping, LineageNode


def _make_node(target: str, sources: list[str], t_type: str = "passthrough") -> LineageNode:
    return LineageNode(
        target_table=target,
        source_tables=sources,
        column_mappings=[ColumnMapping(target_col="id", source_expression="id")],
        transformation_type=t_type,
        raw_sql="INSERT INTO x SELECT id FROM y",
        pipeline_name="test_pipeline",
    )


# ---------------------------------------------------------------------------
# Basic node / edge creation
# ---------------------------------------------------------------------------

class TestGraphConstruction:
    def test_add_node(self):
        g = LineageGraph()
        g.add_node("raw_orders")
        assert "raw_orders" in g.all_tables()

    def test_ingest_creates_nodes_and_edges(self):
        g = LineageGraph()
        node = _make_node("staging.orders", ["source.orders"])
        g.add_lineage_node(node)
        assert "staging.orders" in g.all_tables()
        assert "source.orders" in g.all_tables()
        edges = g.all_edges()
        assert len(edges) == 1
        assert edges[0]["source"] == "source.orders"
        assert edges[0]["target"] == "staging.orders"

    def test_ingest_multiple_sources(self):
        g = LineageGraph()
        node = _make_node("staging.enriched", ["staging.orders", "source.products"])
        g.add_lineage_node(node)
        assert len(g.all_edges()) == 2

    def test_is_source_flag(self):
        g = LineageGraph()
        node = _make_node("staging.orders", ["source.raw"])
        g.add_lineage_node(node)
        # source.raw has no incoming edges → is_source=True
        assert g.node_metadata("source.raw")["is_source"] is True
        # staging.orders has an incoming edge → is_source=False
        assert g.node_metadata("staging.orders")["is_source"] is False

    def test_no_self_loop(self):
        """Nodes should not create self-referencing edges."""
        g = LineageGraph()
        node = _make_node("staging.orders", [])
        g.add_lineage_node(node)
        edges = g.all_edges()
        assert all(e["source"] != e["target"] for e in edges)


# ---------------------------------------------------------------------------
# Upstream / downstream queries
# ---------------------------------------------------------------------------

class TestUpstreamDownstream:
    def _build_chain(self) -> LineageGraph:
        """Build: source.raw → staging.orders → warehouse.daily → mart.sales"""
        g = LineageGraph()
        g.ingest([
            _make_node("staging.orders", ["source.raw"]),
            _make_node("warehouse.daily", ["staging.orders"]),
            _make_node("mart.sales", ["warehouse.daily"]),
        ])
        return g

    def test_upstream_of_leaf(self):
        g = self._build_chain()
        result = g.get_upstream("mart.sales")
        upstream_names = [u["table"] for u in result["upstream"]]
        assert "warehouse.daily" in upstream_names
        assert "staging.orders" in upstream_names
        assert "source.raw" in upstream_names

    def test_downstream_of_root(self):
        g = self._build_chain()
        result = g.get_downstream("source.raw")
        downstream_names = [d["table"] for d in result["downstream"]]
        assert "staging.orders" in downstream_names
        assert "warehouse.daily" in downstream_names
        assert "mart.sales" in downstream_names

    def test_direct_flag(self):
        g = self._build_chain()
        result = g.get_upstream("mart.sales")
        direct = {u["table"]: u["direct"] for u in result["upstream"]}
        assert direct["warehouse.daily"] is True
        assert direct["staging.orders"] is False

    def test_unknown_table_returns_empty(self):
        g = self._build_chain()
        result = g.get_upstream("nonexistent.table")
        assert result["upstream"] == []

    def test_downstream_unknown_table(self):
        g = self._build_chain()
        result = g.get_downstream("nonexistent.table")
        assert result["downstream"] == []


# ---------------------------------------------------------------------------
# Impact analysis
# ---------------------------------------------------------------------------

class TestImpactAnalysis:
    def test_changing_source_affects_all(self):
        g = LineageGraph()
        g.ingest([
            _make_node("staging.orders", ["source.raw"]),
            _make_node("warehouse.daily", ["staging.orders"]),
            _make_node("mart.sales", ["warehouse.daily"]),
        ])
        result = g.get_impact_analysis("source.raw")
        affected = {item["table"] for item in result["affected_tables"]}
        assert "staging.orders" in affected
        assert "warehouse.daily" in affected
        assert "mart.sales" in affected

    def test_changing_mid_node_skips_upstream(self):
        g = LineageGraph()
        g.ingest([
            _make_node("staging.orders", ["source.raw"]),
            _make_node("warehouse.daily", ["staging.orders"]),
        ])
        result = g.get_impact_analysis("staging.orders")
        affected = {item["table"] for item in result["affected_tables"]}
        assert "warehouse.daily" in affected
        assert "source.raw" not in affected

    def test_affected_count(self):
        g = LineageGraph()
        g.ingest([
            _make_node("b", ["a"]),
            _make_node("c", ["a"]),
            _make_node("d", ["b", "c"]),
        ])
        result = g.get_impact_analysis("a")
        assert result["affected_count"] >= 3

    def test_critical_path_present(self):
        g = LineageGraph()
        g.ingest([
            _make_node("staging.orders", ["source.raw"]),
            _make_node("mart.sales", ["staging.orders"]),
        ])
        result = g.get_impact_analysis("source.raw")
        mart_item = next(
            (x for x in result["affected_tables"] if x["table"] == "mart.sales"), None
        )
        assert mart_item is not None
        assert "source.raw" in mart_item["critical_path"]
        assert "mart.sales" in mart_item["critical_path"]


# ---------------------------------------------------------------------------
# Topological order
# ---------------------------------------------------------------------------

class TestTopologicalOrder:
    def test_topological_order_correct(self):
        g = LineageGraph()
        g.ingest([
            _make_node("b", ["a"]),
            _make_node("c", ["b"]),
        ])
        order = g.topological_order()
        assert order.index("a") < order.index("b")
        assert order.index("b") < order.index("c")


# ---------------------------------------------------------------------------
# Edge lineage
# ---------------------------------------------------------------------------

class TestEdgeLineage:
    def test_edge_lineage_returns_column_mappings(self):
        g = LineageGraph()
        node = LineageNode(
            target_table="staging.orders",
            source_tables=["source.raw"],
            column_mappings=[
                ColumnMapping("order_id", "order_id"),
                ColumnMapping("amount", "price * qty"),
            ],
            transformation_type="passthrough",
            raw_sql="...",
        )
        g.add_lineage_node(node)
        edge_data = g.get_edge_lineage("source.raw", "staging.orders")
        assert edge_data is not None
        assert len(edge_data["column_mappings"]) == 2

    def test_unknown_edge_returns_none(self):
        g = LineageGraph()
        g.add_node("a")
        g.add_node("b")
        assert g.get_edge_lineage("a", "b") is None


# ---------------------------------------------------------------------------
# as_dict serialisation
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_as_dict_structure(self):
        g = LineageGraph()
        g.ingest([_make_node("staging.orders", ["source.raw"])])
        d = g.as_dict()
        assert "nodes" in d
        assert "edges" in d
        assert any(n["table"] == "source.raw" for n in d["nodes"])
        assert any(e["source"] == "source.raw" for e in d["edges"])
