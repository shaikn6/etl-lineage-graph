"""networkx-based directed graph builder for data lineage."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import networkx as nx

from lineage.parser import LineageNode


class LineageGraph:
    """
    Directed graph where nodes are tables and edges represent data flow.

    Node attributes:
        schema          - optional schema name
        row_count_estimate - int or None
        last_updated    - ISO datetime string or None
        is_source       - True if the table has no upstream producers

    Edge attributes:
        column_mappings - list of {target_col, source_expression} dicts
        transformation_type - aggregate | join | filter | passthrough
        pipeline_name   - optional string
    """

    def __init__(self) -> None:
        self._g: nx.DiGraph = nx.DiGraph()

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_node(
        self,
        table: str,
        schema: Optional[str] = None,
        row_count_estimate: Optional[int] = None,
        last_updated: Optional[str] = None,
    ) -> None:
        if not self._g.has_node(table):
            self._g.add_node(
                table,
                schema=schema,
                row_count_estimate=row_count_estimate,
                last_updated=last_updated or datetime.utcnow().isoformat(),
                is_source=True,
            )

    def add_lineage_node(self, node: LineageNode) -> None:
        """Ingest a parsed LineageNode into the graph."""
        self.add_node(node.target_table)

        for src in node.source_tables:
            self.add_node(src)
            self._g.add_edge(
                src,
                node.target_table,
                column_mappings=[
                    {
                        "target_col": m.target_col,
                        "source_expression": m.source_expression,
                    }
                    for m in node.column_mappings
                ],
                transformation_type=node.transformation_type,
                pipeline_name=node.pipeline_name,
            )
            # src has an outgoing edge → it may not be a pure sink but stays is_source=True
            # target_table has incoming edges → it's NOT a source
            self._g.nodes[node.target_table]["is_source"] = False

    def ingest(self, nodes: List[LineageNode]) -> None:
        """Ingest a list of LineageNode objects (from parse_sql)."""
        for node in nodes:
            self.add_lineage_node(node)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_upstream(self, table: str) -> Dict[str, Any]:
        """Return all tables that feed into `table` (direct + transitive)."""
        if not self._g.has_node(table):
            return {"table": table, "upstream": []}

        upstream: List[Dict[str, Any]] = []
        for anc in nx.ancestors(self._g, table):
            upstream.append(
                {
                    "table": anc,
                    "direct": self._g.has_edge(anc, table),
                    "attributes": dict(self._g.nodes[anc]),
                }
            )
        return {"table": table, "upstream": upstream}

    def get_downstream(self, table: str) -> Dict[str, Any]:
        """Return all tables that depend on `table` (direct + transitive)."""
        if not self._g.has_node(table):
            return {"table": table, "downstream": []}

        downstream: List[Dict[str, Any]] = []
        for desc in nx.descendants(self._g, table):
            downstream.append(
                {
                    "table": desc,
                    "direct": self._g.has_edge(table, desc),
                    "attributes": dict(self._g.nodes[desc]),
                }
            )
        return {"table": table, "downstream": downstream}

    def get_impact_analysis(self, table: str) -> Dict[str, Any]:
        """
        Answer: 'if `table` changes, which downstream tables are affected?'

        Returns affected tables in topological order with the edge path.
        """
        downstream = self.get_downstream(table)
        affected = downstream["downstream"]

        # Sort by distance from the changed table
        try:
            ordered = sorted(
                affected,
                key=lambda x: nx.shortest_path_length(self._g, table, x["table"]),
            )
        except nx.NetworkXError:
            ordered = affected

        # Attach the critical path for each affected table
        for item in ordered:
            try:
                path = nx.shortest_path(self._g, table, item["table"])
                item["critical_path"] = path
            except nx.NetworkXNoPath:
                item["critical_path"] = []

        return {
            "changed_table": table,
            "affected_count": len(ordered),
            "affected_tables": ordered,
        }

    def topological_order(self) -> List[str]:
        """Return all tables in topological (processing) order."""
        try:
            return list(nx.topological_sort(self._g))
        except nx.NetworkXUnfeasible:
            return list(self._g.nodes)

    def get_edge_lineage(self, source: str, target: str) -> Optional[Dict[str, Any]]:
        """Return column-level lineage for a specific source → target edge."""
        if not self._g.has_edge(source, target):
            return None
        return dict(self._g.edges[source, target])

    def all_tables(self) -> List[str]:
        return list(self._g.nodes)

    def all_edges(self) -> List[Dict[str, Any]]:
        edges = []
        for src, tgt, data in self._g.edges(data=True):
            edges.append({"source": src, "target": tgt, **data})
        return edges

    def node_metadata(self, table: str) -> Optional[Dict[str, Any]]:
        if not self._g.has_node(table):
            return None
        return dict(self._g.nodes[table])

    def as_dict(self) -> Dict[str, Any]:
        """Serialise the full graph to a plain dict (nodes + edges)."""
        return {
            "nodes": [{"table": n, **dict(self._g.nodes[n])} for n in self._g.nodes],
            "edges": self.all_edges(),
        }
