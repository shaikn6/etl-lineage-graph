"""
Cross-system lineage graph merger.

Merges lineage graphs from:
- SQL (V1 LineageGraph / LineageNode)
- Apache Spark (SparkLineageNode)
- dbt models (DbtModelNode)
- Airflow DAGs (AirflowTaskNode — lightweight descriptor)

Produces a unified networkx DiGraph with typed nodes and cross-system edges.

Node types (system attribute):
  SourceTable    — raw source table (SQL origin)
  SparkDataset   — Spark read/write path
  DbtModel       — dbt model node
  AirflowTask    — Airflow task
  SinkTable      — final output / BI table

Cross-system edge detection:
  AirflowTask writes a Parquet path → Spark job reads same path → edge added
  Spark job writes to warehouse table → dbt model refs same table → edge added
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx


# ---------------------------------------------------------------------------
# Unified node descriptor
# ---------------------------------------------------------------------------


class NodeType:
    SOURCE_TABLE = "SourceTable"
    SPARK_DATASET = "SparkDataset"
    DBT_MODEL = "DbtModel"
    AIRFLOW_TASK = "AirflowTask"
    SINK_TABLE = "SinkTable"


@dataclass
class AirflowTaskNode:
    """
    Lightweight descriptor for an Airflow task's lineage.
    No actual Airflow dependency required — pass task metadata directly.
    """

    dag_id: str
    task_id: str
    operator: str = "PythonOperator"
    # Paths/tables this task reads
    input_paths: List[str] = field(default_factory=list)
    # Paths/tables this task writes
    output_paths: List[str] = field(default_factory=list)
    # Upstream task IDs within the same DAG
    upstream_task_ids: List[str] = field(default_factory=list)

    @property
    def node_id(self) -> str:
        return f"{self.dag_id}.{self.task_id}"


# ---------------------------------------------------------------------------
# Cross-system merger
# ---------------------------------------------------------------------------


class CrossSystemMerger:
    """
    Build and query a unified lineage graph spanning multiple systems.

    Usage:
        merger = CrossSystemMerger()
        merger.add_sql_graph(v1_lineage_graph)
        merger.add_spark_nodes(spark_lineage_nodes)
        merger.add_dbt_nodes(dbt_model_nodes)
        merger.add_airflow_tasks(airflow_task_nodes)
        merger.detect_cross_system_edges()
        unified = merger.unified_graph  # nx.DiGraph
    """

    def __init__(self) -> None:
        self._g: nx.DiGraph = nx.DiGraph()
        # Index: path/table_name → node_id (for cross-system matching)
        self._path_index: Dict[str, List[str]] = {}

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def add_sql_graph(self, lineage_graph: Any) -> None:
        """
        Ingest a V1 LineageGraph object.

        Node attributes added: system='sql', node_type=SourceTable|SinkTable
        """
        graph_dict = lineage_graph.as_dict()
        for node in graph_dict.get("nodes", []):
            nid = node["table"]
            is_source = node.get("is_source", True)
            ntype = NodeType.SOURCE_TABLE if is_source else NodeType.SINK_TABLE
            self._add_node(
                node_id=nid,
                label=nid,
                system="sql",
                node_type=ntype,
                metadata=node,
            )

        for edge in graph_dict.get("edges", []):
            src = edge["source"]
            tgt = edge["target"]
            self._g.add_edge(
                src,
                tgt,
                system="sql",
                transformation_type=edge.get("transformation_type", ""),
                column_mappings=edge.get("column_mappings", []),
                pipeline_name=edge.get("pipeline_name", ""),
            )

    def add_spark_nodes(self, spark_nodes: List[Any]) -> None:
        """
        Ingest a list of SparkLineageNode objects from spark_lineage_parser.

        For each node:
        - Source datasets → SparkDataset nodes
        - Sink datasets → SparkDataset nodes (sink)
        - Transformations → SparkDataset intermediate nodes
        - Pipeline-level edges: each source → sink
        """
        from parsers.spark_lineage_parser import SparkLineageNode

        for sn in spark_nodes:
            if not isinstance(sn, SparkLineageNode):
                continue

            source_ids: List[str] = []
            for ds in sn.sources:
                nid = f"spark::{ds.path}"
                self._add_node(
                    node_id=nid,
                    label=ds.path,
                    system="spark",
                    node_type=NodeType.SPARK_DATASET,
                    metadata={
                        "format": ds.format,
                        "dataset_type": "source",
                        "pipeline": sn.pipeline_name,
                    },
                )
                self._register_path(ds.path, nid)
                source_ids.append(nid)

            sink_ids: List[str] = []
            for ds in sn.sinks:
                nid = f"spark::{ds.path}"
                self._add_node(
                    node_id=nid,
                    label=ds.path,
                    system="spark",
                    node_type=NodeType.SPARK_DATASET,
                    metadata={
                        "format": ds.format,
                        "dataset_type": "sink",
                        "pipeline": sn.pipeline_name,
                    },
                )
                self._register_path(ds.path, nid)
                sink_ids.append(nid)

            # Add pipeline edges: each source → each sink
            for src_id in source_ids:
                for sink_id in sink_ids:
                    self._g.add_edge(
                        src_id,
                        sink_id,
                        system="spark",
                        pipeline_name=sn.pipeline_name,
                        transformation_type="spark_pipeline",
                        column_mappings=[],
                    )

            # Transformation-level edges (join inputs → intermediate, etc.)
            for t in sn.transformations:
                if t.operation == "join" and len(t.input_vars) >= 2:
                    left = f"spark::{t.input_vars[0]}"
                    right = f"spark::{t.input_vars[1]}"
                    out = f"spark::{t.output_var}" if t.output_var else None
                    for var_id in [left, right]:
                        if not self._g.has_node(var_id):
                            self._add_node(
                                var_id,
                                t.input_vars[0],
                                "spark",
                                NodeType.SPARK_DATASET,
                                {},
                            )
                        if out:
                            if not self._g.has_node(out):
                                self._add_node(
                                    out,
                                    t.output_var,
                                    "spark",
                                    NodeType.SPARK_DATASET,
                                    {},
                                )
                            self._g.add_edge(
                                var_id,
                                out,
                                system="spark",
                                transformation_type="join",
                                column_mappings=[],
                            )

    def add_dbt_nodes(self, dbt_nodes: List[Any]) -> None:
        """
        Ingest a list of DbtModelNode objects from dbt_lineage_parser.

        For each model:
        - Add a DbtModel node
        - Add edges from ref() and source() dependencies
        - Register warehouse_table path for cross-system linking
        """
        from parsers.dbt_lineage_parser import DbtModelNode

        # First pass: register all model nodes
        model_to_nid: Dict[str, str] = {}
        for mn in dbt_nodes:
            if not isinstance(mn, DbtModelNode):
                continue
            nid = f"dbt::{mn.model_name}"
            model_to_nid[mn.model_name] = nid
            self._add_node(
                node_id=nid,
                label=mn.model_name,
                system="dbt",
                node_type=NodeType.DBT_MODEL,
                metadata={
                    "warehouse_table": mn.warehouse_table,
                    "materialization": mn.materialization,
                    "description": mn.description,
                    "tags": mn.tags,
                    "raw_sql": mn.raw_sql,
                },
            )
            self._register_path(mn.warehouse_table, nid)
            self._register_path(mn.model_name, nid)

        # Second pass: add dependency edges
        for mn in dbt_nodes:
            if not isinstance(mn, DbtModelNode):
                continue
            nid = f"dbt::{mn.model_name}"

            for ref_model in mn.ref_deps:
                ref_nid = model_to_nid.get(ref_model, f"dbt::{ref_model}")
                if not self._g.has_node(ref_nid):
                    self._add_node(ref_nid, ref_model, "dbt", NodeType.DBT_MODEL, {})
                self._g.add_edge(
                    ref_nid,
                    nid,
                    system="dbt",
                    transformation_type="ref",
                    column_mappings=[
                        {
                            "target_col": c.target_col,
                            "source_expression": c.source_expression,
                        }
                        for c in mn.column_lineage
                    ],
                )

            for src_name, tbl_name in mn.source_deps:
                src_id = f"dbt::source::{src_name}.{tbl_name}"
                if not self._g.has_node(src_id):
                    self._add_node(
                        src_id,
                        f"{src_name}.{tbl_name}",
                        "dbt",
                        NodeType.SOURCE_TABLE,
                        {"source_name": src_name, "table_name": tbl_name},
                    )
                self._register_path(f"{src_name}.{tbl_name}", src_id)
                self._g.add_edge(
                    src_id,
                    nid,
                    system="dbt",
                    transformation_type="source",
                    column_mappings=[],
                )

    def add_airflow_tasks(self, tasks: List[AirflowTaskNode]) -> None:
        """
        Ingest Airflow task descriptors.

        For each task:
        - Add an AirflowTask node
        - Add intra-DAG upstream_task_id edges
        - Register input/output paths for cross-system linking
        """
        dag_task_index: Dict[str, str] = {}

        for task in tasks:
            nid = f"airflow::{task.node_id}"
            dag_task_index[task.node_id] = nid
            self._add_node(
                node_id=nid,
                label=f"{task.dag_id}/{task.task_id}",
                system="airflow",
                node_type=NodeType.AIRFLOW_TASK,
                metadata={
                    "dag_id": task.dag_id,
                    "task_id": task.task_id,
                    "operator": task.operator,
                    "input_paths": task.input_paths,
                    "output_paths": task.output_paths,
                },
            )
            for path in task.input_paths:
                self._register_path(path, nid)
            for path in task.output_paths:
                self._register_path(path, nid)

        # Intra-DAG edges
        for task in tasks:
            nid = dag_task_index[task.node_id]
            for upstream_task_id in task.upstream_task_ids:
                up_full = f"{task.dag_id}.{upstream_task_id}"
                up_nid = dag_task_index.get(up_full)
                if up_nid:
                    self._g.add_edge(
                        up_nid,
                        nid,
                        system="airflow",
                        transformation_type="dag_dependency",
                        column_mappings=[],
                    )

    def detect_cross_system_edges(self) -> List[Dict[str, Any]]:
        """
        Scan the path index for shared paths across systems and add cross-system edges.

        Returns:
            List of cross-system edge dicts that were added.
        """
        added_edges: List[Dict[str, Any]] = []

        for path, node_ids in self._path_index.items():
            if len(node_ids) < 2:
                continue

            # Group by system
            by_system: Dict[str, List[str]] = {}
            for nid in node_ids:
                system = self._g.nodes[nid].get("system", "unknown")
                by_system.setdefault(system, []).append(nid)

            if len(by_system) < 2:
                continue  # same system → no cross-system edge needed

            # Determine direction: airflow writes → spark reads, spark writes → dbt reads
            ordered_systems = ["airflow", "sql", "spark", "dbt"]
            flat: List[Tuple[str, str]] = []  # (system, node_id)
            for sys in ordered_systems:
                for nid in by_system.get(sys, []):
                    flat.append((sys, nid))

            # Add edges from each earlier system to later systems
            for i, (sys_a, nid_a) in enumerate(flat):
                for sys_b, nid_b in flat[i + 1 :]:
                    if sys_a != sys_b and not self._g.has_edge(nid_a, nid_b):
                        self._g.add_edge(
                            nid_a,
                            nid_b,
                            system="cross",
                            transformation_type=f"{sys_a}_to_{sys_b}",
                            shared_path=path,
                            column_mappings=[],
                        )
                        added_edges.append(
                            {"source": nid_a, "target": nid_b, "path": path}
                        )

        return added_edges

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def unified_graph(self) -> nx.DiGraph:
        """Return the raw networkx DiGraph."""
        return self._g

    def all_nodes(self) -> List[Dict[str, Any]]:
        return [{"id": n, **dict(self._g.nodes[n])} for n in self._g.nodes]

    def all_edges(self) -> List[Dict[str, Any]]:
        return [{"source": s, "target": t, **d} for s, t, d in self._g.edges(data=True)]

    def nodes_by_system(self, system: str) -> List[Dict[str, Any]]:
        return [
            {"id": n, **dict(self._g.nodes[n])}
            for n in self._g.nodes
            if self._g.nodes[n].get("system") == system
        ]

    def get_end_to_end_path(self, source_id: str, sink_id: str) -> List[List[str]]:
        """Return all simple paths from source to sink in the unified graph."""
        if not self._g.has_node(source_id) or not self._g.has_node(sink_id):
            return []
        try:
            return list(nx.all_simple_paths(self._g, source_id, sink_id))
        except nx.NetworkXError:
            return []

    def as_dict(self) -> Dict[str, Any]:
        return {"nodes": self.all_nodes(), "edges": self.all_edges()}

    def system_coverage_stats(self) -> Dict[str, int]:
        """Return node count per system."""
        stats: Dict[str, int] = {}
        for n in self._g.nodes:
            sys = self._g.nodes[n].get("system", "unknown")
            stats[sys] = stats.get(sys, 0) + 1
        return stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_node(
        self,
        node_id: str,
        label: str,
        system: str,
        node_type: str,
        metadata: Dict[str, Any],
    ) -> None:
        if not self._g.has_node(node_id):
            self._g.add_node(
                node_id,
                label=label,
                system=system,
                node_type=node_type,
                **{
                    k: v
                    for k, v in metadata.items()
                    if k not in ("label", "system", "node_type")
                },
            )

    def _register_path(self, path: str, node_id: str) -> None:
        """Register a path/table name → node_id mapping for cross-system edge detection."""
        if not path:
            return
        # Normalize: strip trailing slash, lowercase
        key = path.rstrip("/").lower()
        if node_id not in self._path_index.get(key, []):
            self._path_index.setdefault(key, []).append(node_id)
