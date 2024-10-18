"""
Generate 4 V2 PNG screenshots demonstrating the new features.
Uses matplotlib to produce clean diagrams without a display server.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx
import numpy as np


SCREENSHOTS_DIR = Path(__file__).parent.parent / "docs" / "screenshots"
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

SYSTEM_COLORS = {
    "sql": "#4A90D9",
    "spark": "#F5A623",
    "dbt": "#7ED321",
    "airflow": "#9B59B6",
}

DARK_BG = "#0f1117"
CARD_BG = "#1a1d27"


# ──────────────────────────────────────────────────────────────────────────────
# 1. Spark AST Lineage
# ──────────────────────────────────────────────────────────────────────────────


def _png1_spark_ast():
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.patch.set_facecolor(DARK_BG)
    fig.suptitle(
        "Spark AST Lineage Parser — Source → Transform → Sink",
        color="white",
        fontsize=14,
        fontweight="bold",
        y=0.97,
    )

    # Left: code snippet
    ax_code = axes[0]
    ax_code.set_facecolor(CARD_BG)
    ax_code.axis("off")
    ax_code.set_title("PySpark Input", color="#90caf9", fontsize=11, pad=8)
    code = (
        "spark = SparkSession.builder\\\n"
        "          .getOrCreate()\n\n"
        "raw_orders = spark.read\\\n"
        "     .parquet('s3://lake/orders/')\n"
        "raw_events = spark.read\\\n"
        "     .parquet('s3://lake/events/')\n\n"
        "clean = raw_orders\\\n"
        "     .filter(\"status='ok'\")\\\n"
        "     .select('order_id','amount')\n\n"
        "joined = clean.join(raw_events,\n"
        "                    on='order_id')\n\n"
        "joined.write.parquet(\n"
        "     's3://lake/processed/')"
    )
    ax_code.text(
        0.04,
        0.96,
        code,
        transform=ax_code.transAxes,
        fontfamily="monospace",
        fontsize=8.5,
        color="#c9d1d9",
        verticalalignment="top",
        linespacing=1.6,
    )

    # Right: extracted lineage graph
    ax_g = axes[1]
    ax_g.set_facecolor(DARK_BG)
    ax_g.set_title("Extracted Lineage", color="#90caf9", fontsize=11, pad=8)
    ax_g.axis("off")

    G = nx.DiGraph()
    nodes = [
        ("s3://lake/orders/", {"color": SYSTEM_COLORS["spark"], "size": 1400}),
        ("s3://lake/events/", {"color": SYSTEM_COLORS["spark"], "size": 1400}),
        ("[filter+select]", {"color": "#555577", "size": 900}),
        ("[join]", {"color": "#555577", "size": 900}),
        ("s3://processed/", {"color": "#E74C3C", "size": 1400}),
    ]
    for nid, _ in nodes:
        G.add_node(nid)
    edges = [
        ("s3://lake/orders/", "[filter+select]"),
        ("[filter+select]", "[join]"),
        ("s3://lake/events/", "[join]"),
        ("[join]", "s3://processed/"),
    ]
    G.add_edges_from(edges)

    pos = {
        "s3://lake/orders/": (0, 1),
        "s3://lake/events/": (0, -1),
        "[filter+select]": (1, 1),
        "[join]": (2, 0),
        "s3://processed/": (3, 0),
    }
    node_colors = [d["color"] for _, d in nodes]
    node_sizes = [d["size"] for _, d in nodes]

    nx.draw_networkx_nodes(
        G, pos, ax=ax_g, node_color=node_colors, node_size=node_sizes
    )
    nx.draw_networkx_edges(
        G,
        pos,
        ax=ax_g,
        edge_color="#555",
        arrows=True,
        arrowsize=15,
        width=1.5,
        connectionstyle="arc3,rad=0.0",
    )
    labels = {n: n if len(n) < 20 else n[:17] + "…" for n in G.nodes}
    nx.draw_networkx_labels(G, pos, labels, ax=ax_g, font_size=7, font_color="white")

    legend = [
        mpatches.Patch(color=SYSTEM_COLORS["spark"], label="SparkDataset"),
        mpatches.Patch(color="#555577", label="Transformation"),
        mpatches.Patch(color="#E74C3C", label="Sink"),
    ]
    ax_g.legend(
        handles=legend,
        loc="lower right",
        fontsize=8,
        facecolor=CARD_BG,
        labelcolor="white",
        framealpha=0.8,
    )

    plt.tight_layout()
    out = SCREENSHOTS_DIR / "spark_ast_lineage.png"
    plt.savefig(out, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close()
    print(f"Saved {out}")


# ──────────────────────────────────────────────────────────────────────────────
# 2. dbt Lineage — ref()/source() dependency graph
# ──────────────────────────────────────────────────────────────────────────────


def _png2_dbt_lineage():
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.patch.set_facecolor(DARK_BG)
    fig.suptitle(
        "dbt Lineage Parser — ref() / source() Dependency Graph",
        color="white",
        fontsize=14,
        fontweight="bold",
        y=0.97,
    )

    ax_sql = axes[0]
    ax_sql.set_facecolor(CARD_BG)
    ax_sql.axis("off")
    ax_sql.set_title(
        "dbt Model SQL (orders_enriched)", color="#90caf9", fontsize=11, pad=8
    )
    dbt_sql = (
        "{{ config(materialized='table') }}\n\n"
        "SELECT\n"
        "    o.order_id,\n"
        "    o.customer_id,\n"
        "    c.full_name,\n"
        "    o.amount,\n"
        "    c.region\n"
        "FROM {{ ref('stg_orders') }} o\n"
        "JOIN {{ ref('stg_customers') }} c\n"
        "  ON o.customer_id = c.id\n\n"
        "-- stg_orders:\n"
        "--   {{ source('raw', 'orders') }}"
    )
    ax_sql.text(
        0.04,
        0.96,
        dbt_sql,
        transform=ax_sql.transAxes,
        fontfamily="monospace",
        fontsize=8.5,
        color="#c9d1d9",
        verticalalignment="top",
        linespacing=1.6,
    )

    ax_g = axes[1]
    ax_g.set_facecolor(DARK_BG)
    ax_g.axis("off")
    ax_g.set_title(
        "Extracted Model Dependency Graph", color="#90caf9", fontsize=11, pad=8
    )

    G = nx.DiGraph()
    models = {
        "raw.orders": {"color": "#888", "y": 2},
        "raw.customers": {"color": "#888", "y": 0},
        "stg_orders": {"color": SYSTEM_COLORS["dbt"], "y": 2},
        "stg_customers": {"color": SYSTEM_COLORS["dbt"], "y": 0},
        "orders_enriched": {"color": SYSTEM_COLORS["dbt"], "y": 1},
        "regional_revenue": {"color": "#E74C3C", "y": 1},
    }
    pos = {
        "raw.orders": (0, 2),
        "raw.customers": (0, 0),
        "stg_orders": (1, 2),
        "stg_customers": (1, 0),
        "orders_enriched": (2, 1),
        "regional_revenue": (3, 1),
    }
    for nid in models:
        G.add_node(nid)
    edges = [
        ("raw.orders", "stg_orders"),
        ("raw.customers", "stg_customers"),
        ("stg_orders", "orders_enriched"),
        ("stg_customers", "orders_enriched"),
        ("orders_enriched", "regional_revenue"),
    ]
    G.add_edges_from(edges)

    colors = [models[n]["color"] for n in G.nodes]
    nx.draw_networkx_nodes(G, pos, ax=ax_g, node_color=colors, node_size=1200)
    nx.draw_networkx_edges(
        G, pos, ax=ax_g, edge_color="#555", arrows=True, arrowsize=15, width=1.5
    )
    nx.draw_networkx_labels(
        G, pos, {n: n for n in G.nodes}, ax=ax_g, font_size=7.5, font_color="white"
    )

    legend = [
        mpatches.Patch(color="#888", label="Source table"),
        mpatches.Patch(color=SYSTEM_COLORS["dbt"], label="dbt model"),
        mpatches.Patch(color="#E74C3C", label="Final output"),
    ]
    ax_g.legend(
        handles=legend,
        loc="lower right",
        fontsize=8,
        facecolor=CARD_BG,
        labelcolor="white",
        framealpha=0.8,
    )

    plt.tight_layout()
    out = SCREENSHOTS_DIR / "dbt_lineage.png"
    plt.savefig(out, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close()
    print(f"Saved {out}")


# ──────────────────────────────────────────────────────────────────────────────
# 3. Cross-system unified graph
# ──────────────────────────────────────────────────────────────────────────────


def _png3_cross_system():
    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor(DARK_BG)
    ax.set_facecolor(DARK_BG)
    ax.set_title(
        "Cross-System Unified Lineage Graph — SQL + Spark + dbt + Airflow",
        color="white",
        fontsize=14,
        fontweight="bold",
        pad=12,
    )
    ax.axis("off")

    G = nx.DiGraph()
    # Airflow
    G.add_node("Airflow: extract_orders", system="airflow")
    G.add_node("Airflow: extract_events", system="airflow")
    G.add_node("Airflow: run_spark", system="airflow")
    # Spark
    G.add_node("Spark: raw/orders", system="spark")
    G.add_node("Spark: raw/events", system="spark")
    G.add_node("Spark: processed/", system="spark")
    # SQL
    G.add_node("SQL: staging.orders", system="sql")
    G.add_node("SQL: mart.revenue", system="sql")
    # dbt
    G.add_node("dbt: stg_orders", system="dbt")
    G.add_node("dbt: orders_enriched", system="dbt")
    G.add_node("dbt: regional_revenue", system="dbt")

    edges = [
        # Airflow DAG edges
        ("Airflow: extract_orders", "Airflow: run_spark", "airflow"),
        ("Airflow: extract_events", "Airflow: run_spark", "airflow"),
        # Cross: Airflow → Spark
        ("Airflow: extract_orders", "Spark: raw/orders", "cross"),
        ("Airflow: extract_events", "Spark: raw/events", "cross"),
        # Spark pipeline
        ("Spark: raw/orders", "Spark: processed/", "spark"),
        ("Spark: raw/events", "Spark: processed/", "spark"),
        # SQL pipeline
        ("SQL: staging.orders", "SQL: mart.revenue", "sql"),
        # Cross: Spark → dbt
        ("Spark: processed/", "dbt: stg_orders", "cross"),
        # dbt pipeline
        ("dbt: stg_orders", "dbt: orders_enriched", "dbt"),
        ("dbt: orders_enriched", "dbt: regional_revenue", "dbt"),
    ]
    for src, tgt, system in edges:
        G.add_edge(src, tgt, system=system)

    pos = {
        "Airflow: extract_orders": (0, 3),
        "Airflow: extract_events": (0, 1),
        "Airflow: run_spark": (0, -0.5),
        "Spark: raw/orders": (2, 3),
        "Spark: raw/events": (2, 1),
        "Spark: processed/": (4, 2),
        "SQL: staging.orders": (2, -1),
        "SQL: mart.revenue": (4, -1),
        "dbt: stg_orders": (6, 3),
        "dbt: orders_enriched": (8, 2),
        "dbt: regional_revenue": (10, 2),
    }

    node_colors = [SYSTEM_COLORS[G.nodes[n]["system"]] for n in G.nodes]
    edge_colors = [
        (
            "#E74C3C"
            if G.edges[e].get("system") == "cross"
            else SYSTEM_COLORS.get(G.edges[e].get("system", "sql"), "#888")
        )
        for e in G.edges
    ]
    edge_styles = [
        "dashed" if G.edges[e].get("system") == "cross" else "solid" for e in G.edges
    ]

    nx.draw_networkx_nodes(
        G, pos, ax=ax, node_color=node_colors, node_size=900, alpha=0.95
    )
    for (src, tgt), ec, style in zip(G.edges, edge_colors, edge_styles):
        nx.draw_networkx_edges(
            G,
            pos,
            edgelist=[(src, tgt)],
            ax=ax,
            edge_color=[ec],
            arrows=True,
            arrowsize=14,
            width=2.0 if style == "dashed" else 1.2,
            style=style,
            connectionstyle="arc3,rad=0.08",
        )

    short = {n: n.split(": ", 1)[1] if ": " in n else n for n in G.nodes}
    nx.draw_networkx_labels(G, pos, short, ax=ax, font_size=7, font_color="white")

    legend = [
        mpatches.Patch(color=SYSTEM_COLORS["airflow"], label="Airflow"),
        mpatches.Patch(color=SYSTEM_COLORS["spark"], label="Spark"),
        mpatches.Patch(color=SYSTEM_COLORS["sql"], label="SQL"),
        mpatches.Patch(color=SYSTEM_COLORS["dbt"], label="dbt"),
        mpatches.Patch(color="#E74C3C", label="Cross-system edge"),
    ]
    ax.legend(
        handles=legend,
        loc="lower right",
        fontsize=9,
        facecolor=CARD_BG,
        labelcolor="white",
        framealpha=0.8,
    )

    plt.tight_layout()
    out = SCREENSHOTS_DIR / "cross_system_lineage.png"
    plt.savefig(out, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close()
    print(f"Saved {out}")


# ──────────────────────────────────────────────────────────────────────────────
# 4. Impact propagation — blast radius heatmap
# ──────────────────────────────────────────────────────────────────────────────


def _png4_impact():
    fig, axes = plt.subplots(1, 2, figsize=(14, 7))
    fig.patch.set_facecolor(DARK_BG)
    fig.suptitle(
        'Impact Propagation — Blast Radius: "orders.amount" renamed',
        color="white",
        fontsize=14,
        fontweight="bold",
        y=0.97,
    )

    # Left: propagation graph
    ax_g = axes[0]
    ax_g.set_facecolor(DARK_BG)
    ax_g.axis("off")
    ax_g.set_title("BFS Propagation Graph", color="#90caf9", fontsize=11, pad=8)

    sev_colors = {
        "BREAKING": "#E74C3C",
        "WARNING": "#F5A623",
        "OK": "#7ED321",
        "SOURCE": "#4A90D9",
    }

    nodes = [
        ("source.orders", "SOURCE", 0, 2),
        ("staging.orders", "BREAKING", 2, 3),
        ("spark::orders", "BREAKING", 2, 1),
        ("dbt::stg_orders", "WARNING", 4, 3),
        ("dbt::enriched", "WARNING", 4, 1),
        ("mart.revenue", "BREAKING", 6, 3),
        ("BI.dashboard", "WARNING", 6, 1),
    ]
    G = nx.DiGraph()
    pos = {}
    for nid, sev, x, y in nodes:
        G.add_node(nid, severity=sev)
        pos[nid] = (x, y)

    edges = [
        ("source.orders", "staging.orders"),
        ("source.orders", "spark::orders"),
        ("staging.orders", "dbt::stg_orders"),
        ("spark::orders", "dbt::enriched"),
        ("dbt::stg_orders", "mart.revenue"),
        ("dbt::enriched", "BI.dashboard"),
    ]
    G.add_edges_from(edges)

    nc = [sev_colors[G.nodes[n]["severity"]] for n in G.nodes]
    nx.draw_networkx_nodes(G, pos, ax=ax_g, node_color=nc, node_size=1000)
    nx.draw_networkx_edges(
        G, pos, ax=ax_g, edge_color="#555", arrows=True, arrowsize=14, width=1.5
    )
    short = {n: n.split("::")[-1] if "::" in n else n for n in G.nodes}
    nx.draw_networkx_labels(G, pos, short, ax=ax_g, font_size=7.5, font_color="white")

    legend = [mpatches.Patch(color=v, label=k) for k, v in sev_colors.items()]
    ax_g.legend(
        handles=legend,
        loc="lower right",
        fontsize=8,
        facecolor=CARD_BG,
        labelcolor="white",
        framealpha=0.8,
    )

    # Right: blast radius table
    ax_t = axes[1]
    ax_t.set_facecolor(DARK_BG)
    ax_t.axis("off")
    ax_t.set_title("Blast Radius Report", color="#90caf9", fontsize=11, pad=8)

    report_lines = [
        "Change:  source.orders.amount → RENAMED",
        "",
        "Total impacted : 6 nodes",
        "  BREAKING     : 2  (staging.orders, mart.revenue)",
        "  WARNING      : 3  (dbt models, BI dashboard)",
        "  OK           : 1",
        "",
        "By system:",
        "  SQL           BREAKING=2",
        "  Spark         BREAKING=1",
        "  dbt           WARNING=2",
        "  Airflow       WARNING=1",
        "",
        "Est. fix time  : 9.4 hrs",
        "",
        "Top risk node:",
        "  source.orders  (out-degree = 5)",
    ]
    ax_t.text(
        0.06,
        0.96,
        "\n".join(report_lines),
        transform=ax_t.transAxes,
        fontfamily="monospace",
        fontsize=9.5,
        color="#c9d1d9",
        verticalalignment="top",
        linespacing=1.6,
    )

    # Severity bar
    categories = ["BREAKING", "WARNING", "OK"]
    values = [2, 3, 1]
    bar_colors = ["#E74C3C", "#F5A623", "#7ED321"]
    bar_ax = ax_t.inset_axes([0.06, 0.05, 0.88, 0.25])
    bar_ax.set_facecolor(CARD_BG)
    x_pos = np.arange(len(categories))
    bar_ax.bar(x_pos, values, color=bar_colors, width=0.5)
    bar_ax.set_xticks(x_pos)
    bar_ax.set_xticklabels(categories)
    bar_ax.set_ylim(0, 4)
    bar_ax.tick_params(colors="white", labelsize=9)
    for spine in bar_ax.spines.values():
        spine.set_edgecolor("#2d3148")

    plt.tight_layout()
    out = SCREENSHOTS_DIR / "impact_propagation.png"
    plt.savefig(out, dpi=120, bbox_inches="tight", facecolor=DARK_BG)
    plt.close()
    print(f"Saved {out}")


if __name__ == "__main__":
    _png1_spark_ast()
    _png2_dbt_lineage()
    _png3_cross_system()
    _png4_impact()
    print("All 4 V2 screenshots generated.")
