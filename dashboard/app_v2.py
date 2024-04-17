"""
ETL Lineage Graph V2 — Multi-tab Streamlit dashboard.

Tabs:
  1. SQL Lineage (V1)       — Classic column-level SQL lineage graph
  2. Cross-System Graph     — Unified SQL + Spark + dbt + Airflow lineage
  3. Impact Analysis        — Blast radius simulation for schema changes
  4. System Comparison      — Node/edge coverage across all 4 systems
"""

from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import streamlit as st
except ImportError:
    raise SystemExit("Install streamlit: pip install streamlit")

import networkx as nx

from lineage.graph import LineageGraph
from lineage.parser import parse_sql

from parsers.spark_lineage_parser import parse_spark_code, SparkLineageNode
from parsers.dbt_lineage_parser import parse_dbt_model, DbtModelNode
from graph.cross_system_merger import CrossSystemMerger, AirflowTaskNode, NodeType
from impact.propagation_simulator import PropagationSimulator, Severity

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="ETL Lineage Graph V2",
    page_icon="🕸",
    layout="wide",
    initial_sidebar_state="collapsed",
)

_CSS = """
<style>
[data-testid="stAppViewContainer"] { background: #0f1117; color: #e0e0e0; }
[data-testid="stHeader"] { background: #0f1117; }
.stTabs [data-baseweb="tab"] { color: #9e9e9e; font-size: 14px; }
.stTabs [aria-selected="true"] { color: #90caf9; border-bottom: 2px solid #90caf9; }
.metric-card { background: #1a1d27; border-radius: 8px; padding: 16px; }
.system-chip { display: inline-block; padding: 3px 10px; border-radius: 10px; font-size: 12px; font-weight: 600; }
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Demo data builders
# ---------------------------------------------------------------------------

_DEMO_SQL = textwrap.dedent("""\
    INSERT INTO staging.orders (order_id, customer_id, amount)
    SELECT o.order_id, o.customer_id, o.price * o.qty AS amount
    FROM source.raw_orders o WHERE o.status = 'confirmed';

    INSERT INTO staging.customers (customer_id, full_name, region)
    SELECT c.customer_id, TRIM(c.first_name || ' ' || c.last_name) AS full_name, c.region
    FROM source.raw_customers c;

    INSERT INTO warehouse.orders_enriched (order_id, customer_id, full_name, amount, region)
    SELECT o.order_id, o.customer_id, c.full_name, o.amount, c.region
    FROM staging.orders o JOIN staging.customers c ON o.customer_id = c.customer_id;

    INSERT INTO mart.regional_revenue (region, total_revenue)
    SELECT region, SUM(amount) AS total_revenue
    FROM warehouse.orders_enriched GROUP BY region;
""")

_DEMO_SPARK = textwrap.dedent("""\
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    raw_orders = spark.read.parquet("s3://datalake/raw/orders/")
    raw_events = spark.read.parquet("s3://datalake/raw/events/")

    orders_clean = raw_orders.filter("status = 'confirmed'").select("order_id", "customer_id", "amount")
    events_clean = raw_events.filter("event_type != 'bot'")

    orders_with_events = orders_clean.join(events_clean, on="order_id", how="left")
    summary = orders_with_events.groupBy("customer_id")

    summary.write.parquet("s3://datalake/processed/order_summary/")
    orders_clean.write.parquet("s3://datalake/processed/clean_orders/")
""")

_DEMO_DBT_ORDERS = textwrap.dedent("""\
    {{ config(materialized='table') }}

    SELECT
        o.order_id,
        o.customer_id,
        c.full_name,
        o.amount,
        c.region
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
""")

_DEMO_DBT_REVENUE = textwrap.dedent("""\
    {{ config(materialized='view') }}

    SELECT
        region,
        SUM(amount) AS total_revenue,
        COUNT(order_id) AS order_count
    FROM {{ ref('orders_enriched') }}
    GROUP BY region
""")

_DEMO_DBT_STG_ORDERS = textwrap.dedent("""\
    {{ config(materialized='view') }}

    SELECT order_id, customer_id, price * qty AS amount, status
    FROM {{ source('raw', 'orders') }}
    WHERE status = 'confirmed'
""")

_DEMO_DBT_STG_CUSTOMERS = textwrap.dedent("""\
    {{ config(materialized='view') }}

    SELECT customer_id, TRIM(first_name || ' ' || last_name) AS full_name, region
    FROM {{ source('raw', 'customers') }}
""")


@st.cache_resource
def _build_demo_data():
    """Build all demo lineage objects (cached across reruns)."""
    # SQL graph
    sql_graph = LineageGraph()
    sql_graph.ingest(parse_sql(_DEMO_SQL, pipeline_name="retail_etl"))

    # Spark nodes
    spark_node = parse_spark_code(_DEMO_SPARK, pipeline_name="spark_orders_pipeline")

    # dbt nodes
    dbt_nodes = [
        parse_dbt_model(_DEMO_DBT_STG_ORDERS, "stg_orders", description="Staged orders"),
        parse_dbt_model(_DEMO_DBT_STG_CUSTOMERS, "stg_customers", description="Staged customers"),
        parse_dbt_model(_DEMO_DBT_ORDERS, "orders_enriched", description="Enriched orders fact"),
        parse_dbt_model(_DEMO_DBT_REVENUE, "regional_revenue", description="Revenue by region"),
    ]

    # Airflow tasks
    airflow_tasks = [
        AirflowTaskNode(
            dag_id="ingest_dag",
            task_id="extract_orders",
            operator="PythonOperator",
            input_paths=["s3://source/orders.csv"],
            output_paths=["s3://datalake/raw/orders/"],
        ),
        AirflowTaskNode(
            dag_id="ingest_dag",
            task_id="extract_events",
            operator="PythonOperator",
            input_paths=["s3://source/events.json"],
            output_paths=["s3://datalake/raw/events/"],
        ),
        AirflowTaskNode(
            dag_id="transform_dag",
            task_id="run_spark_pipeline",
            operator="SparkSubmitOperator",
            input_paths=["s3://datalake/raw/orders/", "s3://datalake/raw/events/"],
            output_paths=["s3://datalake/processed/order_summary/"],
            upstream_task_ids=[],
        ),
    ]

    # Cross-system merger
    merger = CrossSystemMerger()
    merger.add_sql_graph(sql_graph)
    merger.add_spark_nodes([spark_node])
    merger.add_dbt_nodes(dbt_nodes)
    merger.add_airflow_tasks(airflow_tasks)
    cross_edges = merger.detect_cross_system_edges()

    return {
        "sql_graph": sql_graph,
        "spark_node": spark_node,
        "dbt_nodes": dbt_nodes,
        "airflow_tasks": airflow_tasks,
        "merger": merger,
        "cross_edges": cross_edges,
    }


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### ETL Lineage Graph V2")
    st.markdown("Cross-system lineage spanning SQL, Spark, dbt, and Airflow.")
    st.divider()
    st.markdown("**Systems covered**")
    for name, color in [("SQL", "#4A90D9"), ("Spark", "#F5A623"), ("dbt", "#7ED321"), ("Airflow", "#9B59B6")]:
        st.markdown(f'<span class="system-chip" style="background:{color}">{name}</span>', unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4 = st.tabs([
    "SQL Lineage (V1)",
    "Cross-System Graph",
    "Impact Analysis",
    "System Comparison",
])

data = _build_demo_data()

# =============================================================================
# TAB 1 — SQL Lineage (V1)
# =============================================================================

with tab1:
    st.subheader("SQL Column-Level Lineage")
    st.markdown("Parse SQL ETL pipelines to extract table dependencies and column-level mappings.")

    col_left, col_right = st.columns([2, 3])

    with col_left:
        st.markdown("**SQL Input**")
        sql_input = st.text_area("Paste your SQL pipeline:", value=_DEMO_SQL, height=280)
        pipeline_name = st.text_input("Pipeline name (optional)", value="retail_etl")

        if st.button("Parse SQL", type="primary"):
            try:
                graph = LineageGraph()
                nodes = parse_sql(sql_input, pipeline_name=pipeline_name or None)
                graph.ingest(nodes)
                st.session_state["v1_graph"] = graph
                st.session_state["v1_parsed"] = nodes
                st.success(f"Parsed {len(nodes)} statement(s)")
            except Exception as e:
                st.error(f"Parse error: {e}")

    with col_right:
        graph = st.session_state.get("v1_graph", data["sql_graph"])
        gd = graph.as_dict()

        m1, m2, m3 = st.columns(3)
        m1.metric("Tables", len(gd["nodes"]))
        m2.metric("Edges", len(gd["edges"]))
        m3.metric("Source tables", sum(1 for n in gd["nodes"] if n.get("is_source")))

        st.markdown("**Lineage edges**")
        if gd["edges"]:
            rows = []
            for e in gd["edges"]:
                rows.append({
                    "Source": e["source"],
                    "Target": e["target"],
                    "Transform": e.get("transformation_type", ""),
                    "Pipeline": e.get("pipeline_name", ""),
                })
            st.dataframe(rows, use_container_width=True)
        else:
            st.info("No edges yet — parse SQL above.")

    st.divider()
    st.markdown("**Column-level mappings**")
    parsed = st.session_state.get("v1_parsed", data.get("v1_parsed", []))
    if not parsed:
        try:
            parsed = parse_sql(_DEMO_SQL, pipeline_name="retail_etl")
        except Exception:
            parsed = []

    for node in parsed:
        with st.expander(f"{node.target_table} ← {', '.join(node.source_tables) or 'N/A'}"):
            st.markdown(f"**Transform type:** `{node.transformation_type}`")
            if node.column_mappings:
                st.dataframe(
                    [{"Target Column": m.target_col, "Source Expression": m.source_expression} for m in node.column_mappings],
                    use_container_width=True,
                )
            else:
                st.caption("No column mappings extracted.")

# =============================================================================
# TAB 2 — Cross-System Graph
# =============================================================================

with tab2:
    st.subheader("Cross-System Unified Lineage Graph")
    st.markdown("Unified graph spanning SQL, Spark, dbt, and Airflow — with cross-system edge detection.")

    merger = data["merger"]
    stats = merger.system_coverage_stats()

    cols = st.columns(5)
    for idx, (system, color) in enumerate([
        ("sql", "#4A90D9"), ("spark", "#F5A623"), ("dbt", "#7ED321"), ("airflow", "#9B59B6")
    ]):
        count = stats.get(system, 0)
        with cols[idx]:
            st.metric(system.upper(), count, help=f"Nodes from {system} system")

    with cols[4]:
        cross_count = sum(1 for _, _, d in merger.unified_graph.edges(data=True) if d.get("system") == "cross")
        st.metric("Cross-system edges", cross_count)

    st.divider()

    col_graph, col_detail = st.columns([3, 2])

    with col_graph:
        st.markdown("**All nodes**")
        all_nodes = merger.all_nodes()
        display_nodes = []
        system_colors = {"sql": "#4A90D9", "spark": "#F5A623", "dbt": "#7ED321", "airflow": "#9B59B6"}
        for n in all_nodes:
            sys = n.get("system", "unknown")
            color = system_colors.get(sys, "#ccc")
            display_nodes.append({
                "ID": n["id"],
                "Label": n.get("label", ""),
                "System": sys.upper(),
                "Type": n.get("node_type", ""),
            })
        st.dataframe(display_nodes, use_container_width=True, height=300)

    with col_detail:
        st.markdown("**Cross-system edges detected**")
        ce = data["cross_edges"]
        if ce:
            st.dataframe(
                [{"Source": e["source"], "Target": e["target"], "Shared Path": e["path"]} for e in ce],
                use_container_width=True,
            )
        else:
            st.info("No cross-system edges detected in demo data.")

        st.markdown("**Unified edges**")
        all_edges = merger.all_edges()
        st.dataframe(
            [{"Source": e["source"][:40], "Target": e["target"][:40], "System": e.get("system",""), "Transform": e.get("transformation_type","")} for e in all_edges],
            use_container_width=True,
            height=200,
        )

    st.divider()
    st.markdown("**Export visualization**")
    if st.button("Generate cross_system_lineage.html"):
        try:
            from viz.cross_system_viz import CrossSystemViz
            viz = CrossSystemViz(merger.unified_graph)
            out_path = str(Path(__file__).parent.parent / "docs" / "cross_system_lineage.html")
            actual_path = viz.export_html(output_path=out_path)
            st.success(f"Exported to: `{actual_path}`")
        except Exception as e:
            st.error(f"Export failed: {e}")

# =============================================================================
# TAB 3 — Impact Analysis
# =============================================================================

with tab3:
    st.subheader("Change Impact Propagation")
    st.markdown("Simulate the blast radius of a schema change across all 4 systems.")

    merger = data["merger"]
    simulator = PropagationSimulator(merger.unified_graph)

    col_form, col_results = st.columns([1, 2])

    with col_form:
        st.markdown("**Configure change**")
        all_node_ids = [n["id"] for n in merger.all_nodes()]
        # Default to a SQL source table
        default_node = next((n for n in all_node_ids if "source" in n.lower()), all_node_ids[0] if all_node_ids else "")
        selected_node = st.selectbox("Changed node", all_node_ids, index=all_node_ids.index(default_node) if default_node in all_node_ids else 0)
        changed_column = st.text_input("Changed column (leave blank for whole-table)", value="amount")
        change_type = st.selectbox("Change type", ["rename", "drop", "type_change", "add"])

        if st.button("Run simulation", type="primary"):
            report = simulator.simulate(
                node_id=selected_node,
                column_name=changed_column or None,
                change_type=change_type,
            )
            st.session_state["impact_report"] = report

    with col_results:
        report = st.session_state.get("impact_report")
        if report is None:
            # Run with defaults on first render
            report = simulator.simulate(
                node_id=default_node,
                column_name="amount",
                change_type="rename",
            )

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total impacted", report.total_impacted)
        m2.metric("BREAKING", report.breaking_count, delta=None)
        m3.metric("WARNING", report.warning_count)
        m4.metric("Fix est.", f"{report.total_fix_hours:.1f}h")

        if report.impacted_nodes:
            sev_colors = {Severity.BREAKING: "🔴", Severity.WARNING: "🟡", Severity.OK: "🟢"}
            rows = []
            for n in report.impacted_nodes:
                rows.append({
                    "Sev": sev_colors.get(n.severity, ""),
                    "Node": n.label[:40],
                    "System": n.system.upper(),
                    "Hop": n.hop_distance,
                    "Fix hrs": n.fix_hours,
                    "Reason": n.reason[:60],
                })
            st.dataframe(rows, use_container_width=True, height=300)
        else:
            st.info("No downstream nodes impacted.")

    st.divider()
    st.markdown("**Top risk nodes** (most downstream dependents)")
    top_risk = simulator.top_risk_nodes(top_n=8)
    if top_risk:
        st.dataframe(
            [{"Node": r["label"][:40], "System": r["system"].upper(), "Out-degree": r["out_degree"], "Centrality": r["centrality_score"]} for r in top_risk],
            use_container_width=True,
        )

    # By-system breakdown
    if report and report.by_system:
        st.divider()
        st.markdown("**Impact by system**")
        bs_cols = st.columns(len(report.by_system))
        for idx, (sys, counts) in enumerate(sorted(report.by_system.items())):
            with bs_cols[idx]:
                st.markdown(f"**{sys.upper()}**")
                for sev, cnt in counts.items():
                    st.markdown(f"- {sev}: {cnt}")

# =============================================================================
# TAB 4 — System Comparison
# =============================================================================

with tab4:
    st.subheader("System Comparison: SQL vs Spark vs dbt vs Airflow")
    st.markdown("Coverage and node-type breakdown across all 4 lineage systems.")

    merger = data["merger"]
    g = merger.unified_graph

    # Per-system stats
    systems = ["sql", "spark", "dbt", "airflow"]
    colors = {"sql": "#4A90D9", "spark": "#F5A623", "dbt": "#7ED321", "airflow": "#9B59B6"}

    rows = []
    for sys in systems:
        sys_nodes = [n for n in g.nodes if g.nodes[n].get("system") == sys]
        sys_edges = [(s, t) for s, t, d in g.edges(data=True) if d.get("system") == sys]
        node_types = {}
        for n in sys_nodes:
            nt = g.nodes[n].get("node_type", "unknown")
            node_types[nt] = node_types.get(nt, 0) + 1
        rows.append({
            "System": sys.upper(),
            "Nodes": len(sys_nodes),
            "Edges": len(sys_edges),
            "Node Types": ", ".join(f"{k}({v})" for k, v in node_types.items()),
        })

    st.dataframe(rows, use_container_width=True)

    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**SQL (V1) — Column-level lineage**")
        st.markdown("- CTE-aware SQL parsing")
        st.markdown("- INSERT INTO / CREATE TABLE AS support")
        st.markdown("- Column mapping extraction")
        st.markdown("- 4 transformation types: aggregate, join, filter, passthrough")

        st.markdown("**Spark — AST-based extraction**")
        st.markdown("- PySpark source code analysis")
        st.markdown("- Multi-step pipeline tracking")
        st.markdown("- join / select / groupBy / filter / union detection")
        st.markdown("- Intermediate DataFrame variable tracking")

    with col_b:
        st.markdown("**dbt — Model dependency graph**")
        st.markdown("- ref() and source() macro resolution")
        st.markdown("- Column-level lineage from SELECT")
        st.markdown("- Schema.yml description and tag ingestion")
        st.markdown("- Warehouse table mapping")

        st.markdown("**Airflow — Task dependency graph**")
        st.markdown("- Intra-DAG upstream task edges")
        st.markdown("- Input/output path registration")
        st.markdown("- Cross-system edge detection with Spark paths")

    st.divider()
    st.markdown("**Cross-system edge detection logic**")
    st.code(textwrap.dedent("""\
        # CrossSystemMerger.detect_cross_system_edges()
        # Shared path index: normalized path → [node_ids from different systems]
        # Ordering: airflow → sql → spark → dbt
        # E.g.:  Airflow writes s3://datalake/raw/orders/
        #        Spark reads  s3://datalake/raw/orders/
        #        → cross-system edge: airflow::ingest_dag.extract_orders
        #                           → spark::s3://datalake/raw/orders/
    """), language="python")
