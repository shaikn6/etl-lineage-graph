"""
Generate PNG screenshots for the ETL Lineage Graph project.
Run from repo root: python scripts/generate_screenshots.py
"""

import os
import sys

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import networkx as nx

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "screenshots")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def lineage_graph_png() -> None:
    """Draw the full data lineage graph and save as lineage_graph.png."""
    G = nx.DiGraph()
    edges = [
        ("raw.orders", "staging.orders"),
        ("raw.customers", "staging.customers"),
        ("raw.products", "staging.products"),
        ("staging.orders", "mart.daily_sales"),
        ("staging.customers", "mart.daily_sales"),
        ("staging.products", "mart.daily_sales"),
        ("mart.daily_sales", "mart.sales_performance"),
    ]
    G.add_edges_from(edges)

    layer_colors = {
        "raw": "#e74c3c",
        "staging": "#f39c12",
        "mart": "#27ae60",
    }
    node_colors = [layer_colors[n.split(".")[0]] for n in G.nodes()]

    pos = nx.spring_layout(G, seed=42, k=2)

    fig, ax = plt.subplots(figsize=(14, 8))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#16213e")

    nx.draw(
        G,
        pos,
        ax=ax,
        node_color=node_colors,
        node_size=3000,
        font_size=9,
        font_color="white",
        font_weight="bold",
        arrows=True,
        arrowsize=20,
        edge_color="#aaaaaa",
        width=2,
        with_labels=True,
    )

    ax.set_title(
        "ETL Data Lineage Graph — Retail Pipeline",
        fontsize=14,
        fontweight="bold",
        color="white",
        pad=16,
    )

    patches = [
        mpatches.Patch(color="#e74c3c", label="Raw Layer"),
        mpatches.Patch(color="#f39c12", label="Staging Layer"),
        mpatches.Patch(color="#27ae60", label="Mart Layer"),
    ]
    legend = plt.legend(
        handles=patches,
        loc="upper left",
        facecolor="#1a1a2e",
        edgecolor="#555",
        labelcolor="white",
        fontsize=10,
    )

    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, "lineage_graph.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"  saved → {out}")


def impact_analysis_png() -> None:
    """Draw the impact analysis bar chart and save as impact_analysis.png."""
    tables = ["staging.orders", "mart.daily_sales", "mart.sales_performance"]
    distances = [1, 2, 3]
    bar_colors = ["#f1c40f", "#e67e22", "#e74c3c"]

    fig, ax = plt.subplots(figsize=(8, 5))
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#16213e")

    bars = ax.barh(tables, distances, color=bar_colors, height=0.5, edgecolor="#555")

    ax.set_xlabel("Distance from source", color="white", fontsize=11)
    ax.set_title(
        "Impact Analysis: If raw.orders changes…",
        fontsize=13,
        fontweight="bold",
        color="white",
        pad=14,
    )

    ax.tick_params(colors="white", labelsize=10)
    for spine in ax.spines.values():
        spine.set_edgecolor("#555")

    ax.set_xticks([1, 2, 3])
    ax.set_xticklabels(["1", "2", "3"], color="white")
    ax.set_xlim(0, 4)

    # Annotation
    ax.annotate(
        "3 downstream tables affected",
        xy=(3, 2),
        xytext=(3.1, 1.5),
        fontsize=10,
        color="#f1c40f",
        fontweight="bold",
        arrowprops=dict(arrowstyle="->", color="#f1c40f", lw=1.5),
    )

    for bar, dist in zip(bars, distances):
        ax.text(
            dist - 0.07,
            bar.get_y() + bar.get_height() / 2,
            str(dist),
            va="center",
            ha="right",
            color="white",
            fontweight="bold",
            fontsize=11,
        )

    plt.tight_layout()
    out = os.path.join(OUTPUT_DIR, "impact_analysis.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"  saved → {out}")


def api_demo_png() -> None:
    """Render a terminal-style API response screenshot as api_demo.png."""
    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Title bar
    ax.add_patch(
        mpatches.FancyBboxPatch(
            (0.02, 0.88), 0.96, 0.08,
            boxstyle="round,pad=0.01",
            facecolor="#21262d",
            edgecolor="#30363d",
            linewidth=1.5,
        )
    )
    ax.text(
        0.5, 0.92,
        "  terminal — etl-lineage-graph  ",
        ha="center", va="center",
        fontsize=10, color="#8b949e",
        fontfamily="monospace",
    )

    # Terminal dots
    for x, color in [(0.06, "#ff5f56"), (0.09, "#ffbd2e"), (0.12, "#27c93f")]:
        ax.add_patch(plt.Circle((x, 0.92), 0.012, color=color))

    # Code body
    # Each entry: (text, color, bold)  — empty text lines skip rendering
    lines = [
        ("$ curl http://localhost:8000/impact/raw.orders", "#27c93f", False),
        ("", None, False),
        ("GET /impact/raw.orders", "#58a6ff", True),
        ("", None, False),
        ("{", "#c9d1d9", False),
        ('  "table": "raw.orders",', "#c9d1d9", False),
        ('  "upstream": [],', "#c9d1d9", False),
        ('  "downstream": [', "#c9d1d9", False),
        ('    "staging.orders",', "#f0a500", False),
        ('    "mart.daily_sales",', "#e67e22", False),
        ('    "mart.sales_performance"', "#e74c3c", False),
        ("  ],", "#c9d1d9", False),
        ('  "critical_path_length": 3,', "#c9d1d9", False),
        ('  "impact_score": "HIGH"', "#ff7b72", True),
        ("}", "#c9d1d9", False),
    ]

    y = 0.82
    dy = 0.047
    for text, color, bold in lines:
        if text and color:
            ax.text(
                0.05, y, text,
                ha="left", va="top",
                fontsize=8.5,
                color=color,
                fontfamily="monospace",
                fontweight="bold" if bold else "normal",
            )
        y -= dy

    plt.tight_layout(pad=0)
    out = os.path.join(OUTPUT_DIR, "api_demo.png")
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close()
    print(f"  saved → {out}")


if __name__ == "__main__":
    print("Generating screenshots…")
    lineage_graph_png()
    impact_analysis_png()
    api_demo_png()
    print("Done.")
