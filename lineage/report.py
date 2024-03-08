"""Generate Mermaid flowchart strings and HTML lineage reports from the graph."""

from __future__ import annotations

import os
from typing import Optional

from jinja2 import Environment, FileSystemLoader

from lineage.graph import LineageGraph


def generate_mermaid(graph: LineageGraph, direction: str = "LR") -> str:
    """
    Generate a Mermaid flowchart string from the lineage graph.

    Args:
        graph: populated LineageGraph instance
        direction: LR (left-right) | TD (top-down)

    Returns:
        Multi-line Mermaid diagram string.

    Example output:
        flowchart LR
            raw_orders --> staging_orders
            staging_orders --> mart_sales
    """
    lines = [f"flowchart {direction}"]

    # Add node definitions with styling
    for table in graph.all_tables():
        meta = graph.node_metadata(table) or {}
        is_source = meta.get("is_source", True)
        # Source tables get a rounded-rectangle, derived get a stadium shape
        if is_source:
            lines.append(f'    {_node_id(table)}["{table}"]')
        else:
            lines.append(f'    {_node_id(table)}("{table}")')

    lines.append("")  # blank separator

    # Add edges with transformation labels
    for edge in graph.all_edges():
        src_id = _node_id(edge["source"])
        tgt_id = _node_id(edge["target"])
        t_type = edge.get("transformation_type", "")
        label = f"|{t_type}|" if t_type else ""
        lines.append(f"    {src_id} -->{label} {tgt_id}")

    # Styling
    lines.append("")
    lines.append("    classDef source fill:#d4edda,stroke:#28a745,color:#155724")
    lines.append("    classDef derived fill:#cce5ff,stroke:#004085,color:#004085")
    for table in graph.all_tables():
        meta = graph.node_metadata(table) or {}
        cls = "source" if meta.get("is_source", True) else "derived"
        lines.append(f"    class {_node_id(table)} {cls}")

    return "\n".join(lines)


def generate_html_report(
    graph: LineageGraph,
    title: str = "ETL Lineage Report",
    templates_dir: Optional[str] = None,
) -> str:
    """Render the full HTML lineage report using Jinja2."""
    if templates_dir is None:
        templates_dir = os.path.join(os.path.dirname(__file__), "templates")

    env = Environment(loader=FileSystemLoader(templates_dir), autoescape=True)
    template = env.get_template("lineage_report.html")

    mermaid_diagram = generate_mermaid(graph)
    nodes = graph.all_tables()
    edges = graph.all_edges()

    # Build table-level summary rows
    table_rows = []
    for table in nodes:
        meta = graph.node_metadata(table) or {}
        upstream = graph.get_upstream(table)["upstream"]
        downstream = graph.get_downstream(table)["downstream"]
        table_rows.append(
            {
                "name": table,
                "is_source": meta.get("is_source", True),
                "upstream_count": len(upstream),
                "downstream_count": len(downstream),
                "last_updated": meta.get("last_updated", "—"),
            }
        )

    return template.render(
        title=title,
        mermaid_diagram=mermaid_diagram,
        table_rows=table_rows,
        edges=edges,
        node_count=len(nodes),
        edge_count=len(edges),
    )


def _node_id(table_name: str) -> str:
    """Convert table name to a safe Mermaid node identifier."""
    return table_name.replace(".", "_").replace("-", "_").replace(" ", "_")
