#!/usr/bin/env python3
"""
run_example.py — Parse all 5 pipeline SQL files, build the lineage graph,
and print a lineage report to stdout.

Run from the repo root:
    pip install -r requirements.txt
    python examples/run_example.py
"""

from __future__ import annotations

import os
import sys

# Make sure the package is importable without installing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lineage.graph import LineageGraph
from lineage.parser import parse_sql
from lineage.report import generate_mermaid

SQL_DIR = os.path.join(os.path.dirname(__file__), "pipeline_sqls")


def load_sql_files() -> list[tuple[str, str]]:
    """Return sorted list of (filename, sql_content) tuples."""
    files = sorted(f for f in os.listdir(SQL_DIR) if f.endswith(".sql"))
    result = []
    for fname in files:
        path = os.path.join(SQL_DIR, fname)
        with open(path, "r") as fh:
            result.append((fname, fh.read()))
    return result


def main() -> None:
    print("=" * 65)
    print("  ETL Lineage Graph — Example Run")
    print("=" * 65)

    graph = LineageGraph()
    sql_files = load_sql_files()

    print(f"\nParsing {len(sql_files)} SQL files from examples/pipeline_sqls/\n")

    all_nodes = []
    for fname, sql in sql_files:
        pipeline_name = fname.replace(".sql", "")
        nodes = parse_sql(sql, pipeline_name=pipeline_name)
        all_nodes.extend(nodes)
        for node in nodes:
            print(f"  [{fname}]")
            print(f"    target  : {node.target_table}")
            print(f"    sources : {node.source_tables}")
            print(f"    type    : {node.transformation_type}")
            if node.column_mappings:
                print(f"    columns : {len(node.column_mappings)} mappings")
                for m in node.column_mappings[:3]:
                    print(f"              {m.target_col:<25} <- {m.source_expression}")
                if len(node.column_mappings) > 3:
                    print(f"              ... +{len(node.column_mappings) - 3} more")
            print()

    graph.ingest(all_nodes)

    print("-" * 65)
    print(
        f"Graph built: {len(graph.all_tables())} tables, {len(graph.all_edges())} edges"
    )
    print("-" * 65)

    # Topological order
    order = graph.topological_order()
    print("\nProcessing order (topological sort):")
    for i, table in enumerate(order, 1):
        print(f"  {i:2}. {table}")

    # Impact analysis on a source table
    print("\n" + "=" * 65)
    impact_table = "orders"
    print(f"Impact analysis: what breaks if '{impact_table}' changes?")
    print("=" * 65)
    impact = graph.get_impact_analysis(impact_table)
    if impact["affected_tables"]:
        for item in impact["affected_tables"]:
            path = " → ".join(item.get("critical_path", [item["table"]]))
            print(f"  AFFECTED: {item['table']}")
            print(f"    path  : {path}")
    else:
        print("  (no downstream tables found)")

    # Upstream of final mart table
    print("\n" + "=" * 65)
    mart_table = "sales_performance"
    print(f"Full upstream lineage of '{mart_table}':")
    print("=" * 65)
    upstream = graph.get_upstream(mart_table)
    for item in upstream["upstream"]:
        tag = "(direct)" if item["direct"] else "(transitive)"
        print(f"  {item['table']:<40} {tag}")

    # Mermaid diagram
    print("\n" + "=" * 65)
    print("Mermaid flowchart (paste into https://mermaid.live):")
    print("=" * 65)
    print(generate_mermaid(graph))

    print("\nDone. To launch the API server:")
    print("  uvicorn lineage.api:app --reload --port 8000")
    print("  Then open http://localhost:8000/report for the HTML report.")


if __name__ == "__main__":
    main()
