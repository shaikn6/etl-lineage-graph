"""Tests for graph/cross_system_merger.py — cross-system lineage graph merging."""

from __future__ import annotations

import pytest

from lineage.graph import LineageGraph
from lineage.parser import ColumnMapping, LineageNode

from graph.cross_system_merger import AirflowTaskNode, CrossSystemMerger, NodeType
from parsers.dbt_lineage_parser import parse_dbt_model
from parsers.spark_lineage_parser import parse_spark_code


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _sql_graph() -> LineageGraph:
    g = LineageGraph()
    g.ingest(
        [
            LineageNode(
                target_table="staging.orders",
                source_tables=["source.raw_orders"],
                column_mappings=[
                    ColumnMapping("order_id", "order_id"),
                    ColumnMapping("amount", "price*qty"),
                ],
                transformation_type="filter",
                raw_sql="INSERT INTO staging.orders SELECT order_id, price*qty AS amount FROM source.raw_orders",
                pipeline_name="etl",
            ),
            LineageNode(
                target_table="mart.revenue",
                source_tables=["staging.orders"],
                column_mappings=[ColumnMapping("total", "SUM(amount)")],
                transformation_type="aggregate",
                raw_sql="INSERT INTO mart.revenue SELECT SUM(amount) AS total FROM staging.orders GROUP BY 1",
                pipeline_name="etl",
            ),
        ]
    )
    return g


def _spark_node():
    code = """
spark = None
import types
spark = types.SimpleNamespace()
spark.read = types.SimpleNamespace()
"""
    # Use direct SparkLineageNode construction for deterministic test data
    from parsers.spark_lineage_parser import SparkLineageNode, SparkDataset

    return SparkLineageNode(
        pipeline_name="spark_etl",
        sources=[
            SparkDataset(
                path="s3://lake/raw/orders/", format="parquet", dataset_type="source"
            ),
            SparkDataset(
                path="s3://lake/raw/events/", format="parquet", dataset_type="source"
            ),
        ],
        sinks=[
            SparkDataset(
                path="s3://lake/processed/orders/",
                format="parquet",
                dataset_type="sink",
            ),
        ],
        transformations=[],
        intermediate_vars=[],
        raw_code="# PySpark pipeline",
    )


def _dbt_nodes():
    return [
        parse_dbt_model(
            "SELECT * FROM {{ ref('stg_orders') }}",
            "orders_fact",
            database="prod",
            schema="analytics",
        ),
        parse_dbt_model(
            "SELECT * FROM {{ source('raw', 'orders') }}",
            "stg_orders",
            database="prod",
            schema="analytics",
        ),
    ]


def _airflow_tasks():
    return [
        AirflowTaskNode(
            dag_id="ingest_dag",
            task_id="load_orders",
            operator="PythonOperator",
            input_paths=["s3://source/orders.csv"],
            output_paths=["s3://lake/raw/orders/"],
        ),
        AirflowTaskNode(
            dag_id="ingest_dag",
            task_id="run_spark",
            operator="SparkSubmitOperator",
            input_paths=["s3://lake/raw/orders/"],
            output_paths=["s3://lake/processed/orders/"],
            upstream_task_ids=["load_orders"],
        ),
    ]


# ---------------------------------------------------------------------------
# SQL graph ingestion
# ---------------------------------------------------------------------------


class TestSQLIngestion:
    def test_sql_nodes_added(self):
        m = CrossSystemMerger()
        m.add_sql_graph(_sql_graph())
        node_ids = [n["id"] for n in m.all_nodes()]
        assert any("staging.orders" in nid for nid in node_ids)
        assert any("source.raw_orders" in nid for nid in node_ids)

    def test_sql_edges_added(self):
        m = CrossSystemMerger()
        m.add_sql_graph(_sql_graph())
        edges = m.all_edges()
        assert any(e["system"] == "sql" for e in edges)

    def test_sql_node_system_attribute(self):
        m = CrossSystemMerger()
        m.add_sql_graph(_sql_graph())
        nodes = {n["id"]: n for n in m.all_nodes()}
        assert nodes["source.raw_orders"]["system"] == "sql"


# ---------------------------------------------------------------------------
# Spark ingestion
# ---------------------------------------------------------------------------


class TestSparkIngestion:
    def test_spark_source_nodes_added(self):
        m = CrossSystemMerger()
        m.add_spark_nodes([_spark_node()])
        node_ids = [n["id"] for n in m.all_nodes()]
        assert any("s3://lake/raw/orders/" in nid for nid in node_ids)

    def test_spark_sink_nodes_added(self):
        m = CrossSystemMerger()
        m.add_spark_nodes([_spark_node()])
        node_ids = [n["id"] for n in m.all_nodes()]
        assert any("s3://lake/processed/orders/" in nid for nid in node_ids)

    def test_spark_edges_source_to_sink(self):
        m = CrossSystemMerger()
        m.add_spark_nodes([_spark_node()])
        edges = m.all_edges()
        assert any(e["system"] == "spark" for e in edges)

    def test_spark_node_system_attribute(self):
        m = CrossSystemMerger()
        m.add_spark_nodes([_spark_node()])
        spark_nodes = m.nodes_by_system("spark")
        assert len(spark_nodes) > 0
        assert all(n["system"] == "spark" for n in spark_nodes)


# ---------------------------------------------------------------------------
# dbt ingestion
# ---------------------------------------------------------------------------


class TestDbtIngestion:
    def test_dbt_model_nodes_added(self):
        m = CrossSystemMerger()
        m.add_dbt_nodes(_dbt_nodes())
        node_ids = [n["id"] for n in m.all_nodes()]
        assert any("orders_fact" in nid for nid in node_ids)

    def test_dbt_ref_edge_added(self):
        m = CrossSystemMerger()
        m.add_dbt_nodes(_dbt_nodes())
        edges = m.all_edges()
        dbt_edges = [e for e in edges if e.get("system") == "dbt"]
        assert len(dbt_edges) > 0

    def test_dbt_source_edge_added(self):
        m = CrossSystemMerger()
        m.add_dbt_nodes(_dbt_nodes())
        edges = m.all_edges()
        source_edges = [e for e in edges if e.get("transformation_type") == "source"]
        assert len(source_edges) > 0


# ---------------------------------------------------------------------------
# Airflow ingestion
# ---------------------------------------------------------------------------


class TestAirflowIngestion:
    def test_airflow_tasks_added(self):
        m = CrossSystemMerger()
        m.add_airflow_tasks(_airflow_tasks())
        airflow_nodes = m.nodes_by_system("airflow")
        assert len(airflow_nodes) == 2

    def test_airflow_intra_dag_edges(self):
        m = CrossSystemMerger()
        m.add_airflow_tasks(_airflow_tasks())
        edges = [e for e in m.all_edges() if e.get("system") == "airflow"]
        assert len(edges) >= 1

    def test_airflow_task_node_id_format(self):
        m = CrossSystemMerger()
        m.add_airflow_tasks(_airflow_tasks())
        node_ids = [n["id"] for n in m.all_nodes()]
        assert any("ingest_dag.load_orders" in nid for nid in node_ids)


# ---------------------------------------------------------------------------
# Cross-system edge detection
# ---------------------------------------------------------------------------


class TestCrossSystemEdges:
    def _merger_with_all(self) -> CrossSystemMerger:
        m = CrossSystemMerger()
        m.add_sql_graph(_sql_graph())
        m.add_spark_nodes([_spark_node()])
        m.add_dbt_nodes(_dbt_nodes())
        m.add_airflow_tasks(_airflow_tasks())
        return m

    def test_detect_returns_list(self):
        m = self._merger_with_all()
        result = m.detect_cross_system_edges()
        assert isinstance(result, list)

    def test_airflow_to_spark_edge_detected(self):
        m = self._merger_with_all()
        cross_edges = m.detect_cross_system_edges()
        # Airflow task writes s3://lake/raw/orders/ → Spark reads same path
        found = any(
            "airflow" in e["source"] and "spark" in e["target"] for e in cross_edges
        )
        assert found, f"Expected airflow→spark edge; got: {cross_edges}"

    def test_cross_edge_has_shared_path(self):
        m = self._merger_with_all()
        cross_edges = m.detect_cross_system_edges()
        for edge in cross_edges:
            assert "path" in edge


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


class TestMergerUtilities:
    def test_system_coverage_stats(self):
        m = CrossSystemMerger()
        m.add_sql_graph(_sql_graph())
        m.add_spark_nodes([_spark_node()])
        stats = m.system_coverage_stats()
        assert "sql" in stats
        assert "spark" in stats
        assert stats["sql"] > 0
        assert stats["spark"] > 0

    def test_as_dict_has_nodes_and_edges(self):
        m = CrossSystemMerger()
        m.add_sql_graph(_sql_graph())
        d = m.as_dict()
        assert "nodes" in d
        assert "edges" in d

    def test_empty_merger_has_no_nodes(self):
        m = CrossSystemMerger()
        assert m.all_nodes() == []
        assert m.all_edges() == []
