"""FastAPI application exposing lineage graph endpoints."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from lineage.graph import LineageGraph
from lineage.parser import parse_sql
from lineage.report import generate_html_report, generate_mermaid

# ---------------------------------------------------------------------------
# Application setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="ETL Lineage Graph",
    description=(
        "Auto-discovers column-level data lineage from SQL/ETL pipelines "
        "using directed graph analysis."
    ),
    version="0.1.0",
)

# In-process graph instance (replace with DB-backed variant for production)
_graph: LineageGraph = LineageGraph()


def get_graph() -> LineageGraph:
    return _graph


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ParseRequest(BaseModel):
    sql: str
    pipeline_name: Optional[str] = None


class ParseResponse(BaseModel):
    parsed_statements: int
    tables_added: int
    edges_added: int


class HealthResponse(BaseModel):
    status: str
    node_count: int
    edge_count: int


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse, tags=["meta"])
def health() -> Dict[str, Any]:
    """Service health check."""
    g = get_graph()
    return {
        "status": "ok",
        "node_count": len(g.all_tables()),
        "edge_count": len(g.all_edges()),
    }


@app.post("/parse", response_model=ParseResponse, tags=["lineage"])
def parse_endpoint(body: ParseRequest) -> Dict[str, Any]:
    """
    Parse a SQL string and add extracted lineage to the in-memory graph.

    Accepts single or multi-statement SQL (semicolon-separated).
    """
    g = get_graph()
    before_tables = set(g.all_tables())
    before_edges = len(g.all_edges())

    nodes = parse_sql(body.sql, pipeline_name=body.pipeline_name)
    g.ingest(nodes)

    after_tables = set(g.all_tables())
    after_edges = len(g.all_edges())

    return {
        "parsed_statements": len(nodes),
        "tables_added": len(after_tables - before_tables),
        "edges_added": after_edges - before_edges,
    }


@app.get("/lineage/{table}", tags=["lineage"])
def get_lineage(table: str) -> Dict[str, Any]:
    """Return upstream and downstream tables for a given table."""
    g = get_graph()
    if table not in g.all_tables():
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found in graph.")
    return {
        "table": table,
        **g.get_upstream(table),
        **g.get_downstream(table),
    }


@app.get("/upstream/{table}", tags=["lineage"])
def get_upstream(table: str) -> Dict[str, Any]:
    """Return all upstream (ancestor) tables."""
    g = get_graph()
    if table not in g.all_tables():
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
    return g.get_upstream(table)


@app.get("/downstream/{table}", tags=["lineage"])
def get_downstream(table: str) -> Dict[str, Any]:
    """Return all downstream (descendant) tables."""
    g = get_graph()
    if table not in g.all_tables():
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
    return g.get_downstream(table)


@app.get("/impact/{table}", tags=["lineage"])
def get_impact(table: str) -> Dict[str, Any]:
    """
    Impact analysis: which downstream tables would be affected if this table changes?

    Returns affected tables in topological order with the critical path.
    """
    g = get_graph()
    if table not in g.all_tables():
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
    return g.get_impact_analysis(table)


@app.get("/graph", tags=["lineage"])
def get_full_graph() -> Dict[str, Any]:
    """Return the full lineage graph (all nodes + edges)."""
    return get_graph().as_dict()


@app.get("/tables", tags=["lineage"])
def list_tables() -> Dict[str, Any]:
    """List all tables currently tracked in the graph."""
    g = get_graph()
    return {"tables": g.all_tables(), "count": len(g.all_tables())}


@app.get("/mermaid", tags=["report"], response_class=HTMLResponse)
def get_mermaid(direction: str = "LR") -> str:
    """Return raw Mermaid diagram source as plain text."""
    return generate_mermaid(get_graph(), direction=direction)


@app.get("/report", tags=["report"], response_class=HTMLResponse)
def get_report(request: Request) -> HTMLResponse:
    """Render the full interactive HTML lineage report."""
    html = generate_html_report(get_graph(), title="ETL Lineage Report")
    return HTMLResponse(content=html, status_code=200)
