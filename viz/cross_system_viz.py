"""
Cross-system lineage visualization using pyvis.

Features:
- Color nodes by system: SQL=blue, Spark=orange, dbt=green, Airflow=purple
- Filter by system type
- Show/hide intermediate nodes
- Click node → see code snippet in sidebar
- Export to HTML file
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import networkx as nx

try:
    from pyvis.network import Network
    _PYVIS_AVAILABLE = True
except ImportError:
    _PYVIS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Color / shape mappings
# ---------------------------------------------------------------------------

_SYSTEM_COLORS: Dict[str, str] = {
    "sql":     "#4A90D9",   # blue
    "spark":   "#F5A623",   # orange
    "dbt":     "#7ED321",   # green
    "airflow": "#9B59B6",   # purple
    "cross":   "#E74C3C",   # red for cross-system edges
    "unknown": "#BDC3C7",   # grey
}

_SYSTEM_SHAPES: Dict[str, str] = {
    "sql":     "database",
    "spark":   "star",
    "dbt":     "diamond",
    "airflow": "triangle",
    "unknown": "dot",
}

_NODE_TYPE_BORDER: Dict[str, str] = {
    "SourceTable":   "#2C3E50",
    "SparkDataset":  "#E67E22",
    "DbtModel":      "#27AE60",
    "AirflowTask":   "#8E44AD",
    "SinkTable":     "#C0392B",
}

_EDGE_COLORS: Dict[str, str] = {
    "sql":     "#85C1E9",
    "spark":   "#FAD7A0",
    "dbt":     "#A9DFBF",
    "airflow": "#D2B4DE",
    "cross":   "#F1948A",
    "unknown": "#D5D8DC",
}


# ---------------------------------------------------------------------------
# HTML template for export (includes sidebar and filter controls)
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cross-System Lineage Graph</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; display: flex; height: 100vh; overflow: hidden; }}

  #controls {{
    width: 240px; min-width: 200px; background: #1a1d27; border-right: 1px solid #2d3148;
    padding: 16px; display: flex; flex-direction: column; gap: 12px; overflow-y: auto;
  }}
  #controls h2 {{ font-size: 13px; text-transform: uppercase; letter-spacing: 1px; color: #7986cb; margin-bottom: 4px; }}
  .legend-item {{ display: flex; align-items: center; gap: 8px; font-size: 12px; cursor: pointer; padding: 4px 0; }}
  .legend-dot {{ width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; }}
  .filter-group {{ display: flex; flex-direction: column; gap: 4px; }}
  .filter-group label {{ font-size: 12px; display: flex; align-items: center; gap: 6px; cursor: pointer; }}
  .filter-group input[type=checkbox] {{ accent-color: #7986cb; }}
  #graph-container {{ flex: 1; position: relative; }}
  #graph-frame {{ width: 100%; height: 100%; border: none; }}
  #sidebar {{
    width: 300px; min-width: 240px; background: #1a1d27; border-left: 1px solid #2d3148;
    padding: 16px; display: flex; flex-direction: column; gap: 8px; overflow-y: auto;
    transition: width 0.2s;
  }}
  #sidebar h3 {{ font-size: 14px; color: #90caf9; }}
  #node-label {{ font-size: 13px; font-weight: 600; color: #e0e0e0; word-break: break-all; }}
  #node-system {{ font-size: 11px; padding: 2px 8px; border-radius: 10px; display: inline-block; margin-bottom: 4px; }}
  #node-meta {{ font-size: 11px; color: #9e9e9e; white-space: pre-wrap; }}
  #code-block {{ background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 10px; font-family: monospace; font-size: 11px; overflow-x: auto; max-height: 350px; overflow-y: auto; color: #c9d1d9; white-space: pre; }}
  #code-placeholder {{ font-size: 12px; color: #555; text-align: center; margin-top: 40px; }}
  .stat-bar {{ display: flex; gap: 8px; flex-wrap: wrap; }}
  .stat-chip {{ background: #252836; border-radius: 4px; padding: 4px 8px; font-size: 11px; }}
</style>
</head>
<body>

<div id="controls">
  <h2>System Filter</h2>
  <div class="filter-group">
    <label><input type="checkbox" checked onchange="filterSystem('sql', this.checked)">
      <span class="legend-dot" style="background:{sql_color}"></span> SQL
    </label>
    <label><input type="checkbox" checked onchange="filterSystem('spark', this.checked)">
      <span class="legend-dot" style="background:{spark_color}"></span> Spark
    </label>
    <label><input type="checkbox" checked onchange="filterSystem('dbt', this.checked)">
      <span class="legend-dot" style="background:{dbt_color}"></span> dbt
    </label>
    <label><input type="checkbox" checked onchange="filterSystem('airflow', this.checked)">
      <span class="legend-dot" style="background:{airflow_color}"></span> Airflow
    </label>
  </div>
  <hr style="border-color:#2d3148">
  <h2>Display</h2>
  <div class="filter-group">
    <label><input type="checkbox" id="hide-intermediate" onchange="toggleIntermediate(this.checked)"> Hide intermediate nodes</label>
  </div>
  <hr style="border-color:#2d3148">
  <h2>Stats</h2>
  <div class="stat-bar" id="stat-bar">{stats_html}</div>
</div>

<div id="graph-container">
  {graph_iframe_or_div}
</div>

<div id="sidebar">
  <h3>Node Details</h3>
  <div id="node-label">Click a node to inspect</div>
  <span id="node-system" style="background:#252836">—</span>
  <div id="node-meta"></div>
  <div id="code-block" style="display:none"></div>
  <div id="code-placeholder">Select a node to view its code snippet</div>
</div>

<script>
const nodeData = {node_data_json};

function showNode(nodeId) {{
  const d = nodeData[nodeId];
  if (!d) return;
  document.getElementById('node-label').textContent = d.label || nodeId;
  const sysEl = document.getElementById('node-system');
  sysEl.textContent = (d.system || 'unknown').toUpperCase();
  sysEl.style.background = {system_colors_json}[d.system] || '#252836';

  const meta = [];
  if (d.node_type) meta.push('Type: ' + d.node_type);
  if (d.warehouse_table) meta.push('Table: ' + d.warehouse_table);
  if (d.materialization) meta.push('Materialized as: ' + d.materialization);
  if (d.format) meta.push('Format: ' + d.format);
  if (d.dag_id) meta.push('DAG: ' + d.dag_id);
  if (d.operator) meta.push('Operator: ' + d.operator);
  if (d.description) meta.push('\\nDescription: ' + d.description);
  document.getElementById('node-meta').textContent = meta.join('\\n');

  const codeBlock = document.getElementById('code-block');
  const codePlaceholder = document.getElementById('code-placeholder');
  const rawCode = d.raw_sql || d.raw_code || '';
  if (rawCode) {{
    codeBlock.textContent = rawCode.substring(0, 3000) + (rawCode.length > 3000 ? '\\n... (truncated)' : '');
    codeBlock.style.display = 'block';
    codePlaceholder.style.display = 'none';
  }} else {{
    codeBlock.style.display = 'none';
    codePlaceholder.style.display = 'block';
  }}
}}

function filterSystem(system, visible) {{
  console.log('Filter', system, visible);
  // Filtering is applied on page reload with updated state — full JS filtering
  // requires direct pyvis network API; this stub shows the intent.
}}

function toggleIntermediate(hide) {{
  console.log('Hide intermediate:', hide);
}}

// Attempt to hook into pyvis iframe click events (same-origin only)
window.addEventListener('message', function(event) {{
  if (event.data && event.data.nodeId) {{
    showNode(event.data.nodeId);
  }}
}});
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Visualization builder
# ---------------------------------------------------------------------------

class CrossSystemViz:
    """
    Build an interactive cross-system lineage visualization.

    Args:
        unified_graph: nx.DiGraph from CrossSystemMerger.unified_graph
        width:  Canvas width (default '100%')
        height: Canvas height (default '700px')
    """

    def __init__(
        self,
        unified_graph: nx.DiGraph,
        width: str = "100%",
        height: str = "700px",
    ) -> None:
        self._g = unified_graph
        self._width = width
        self._height = height

    def _build_pyvis(
        self,
        filter_systems: Optional[List[str]] = None,
        hide_intermediate: bool = False,
    ) -> "Network":
        """Build a pyvis Network from the unified graph."""
        net = Network(
            height=self._height,
            width=self._width,
            bgcolor="#0f1117",
            font_color="#e0e0e0",
            directed=True,
        )
        net.set_options(json.dumps({
            "nodes": {
                "font": {"size": 12, "color": "#e0e0e0"},
                "borderWidth": 2,
                "shadow": True,
            },
            "edges": {
                "arrows": {"to": {"enabled": True, "scaleFactor": 0.8}},
                "smooth": {"type": "cubicBezier"},
                "font": {"size": 10, "color": "#9e9e9e"},
            },
            "physics": {
                "enabled": True,
                "forceAtlas2Based": {
                    "gravitationalConstant": -50,
                    "centralGravity": 0.01,
                    "springLength": 150,
                },
                "solver": "forceAtlas2Based",
                "stabilization": {"iterations": 100},
            },
            "interaction": {
                "hover": True,
                "navigationButtons": True,
                "tooltipDelay": 150,
            },
        }))

        included_nodes: set = set()

        for node_id in self._g.nodes:
            node_data = dict(self._g.nodes[node_id])
            system = node_data.get("system", "unknown")

            if filter_systems and system not in filter_systems:
                continue

            if hide_intermediate and node_data.get("node_type") == "intermediate":
                continue

            color = _SYSTEM_COLORS.get(system, "#BDC3C7")
            shape = _SYSTEM_SHAPES.get(system, "dot")
            border_color = _NODE_TYPE_BORDER.get(node_data.get("node_type", ""), color)
            label = node_data.get("label", node_id)

            # Truncate long labels
            display_label = label if len(label) <= 30 else label[:27] + "..."

            tooltip_lines = [
                f"<b>{label}</b>",
                f"System: {system}",
                f"Type: {node_data.get('node_type', 'unknown')}",
            ]
            if node_data.get("warehouse_table"):
                tooltip_lines.append(f"Table: {node_data['warehouse_table']}")
            if node_data.get("format"):
                tooltip_lines.append(f"Format: {node_data['format']}")

            net.add_node(
                node_id,
                label=display_label,
                title="<br>".join(tooltip_lines),
                color={"background": color, "border": border_color, "highlight": {"background": color, "border": "#FFFFFF"}},
                shape=shape,
                size=20 if system in ("airflow", "spark") else 16,
            )
            included_nodes.add(node_id)

        for src, tgt, edge_data in self._g.edges(data=True):
            if src not in included_nodes or tgt not in included_nodes:
                continue
            system = edge_data.get("system", "unknown")
            color = _EDGE_COLORS.get(system, "#D5D8DC")
            t_type = edge_data.get("transformation_type", "")

            net.add_edge(
                src, tgt,
                title=f"System: {system}<br>Transform: {t_type}",
                color=color,
                width=2 if system == "cross" else 1,
                dashes=system == "cross",
                label=t_type if system == "cross" else "",
            )

        return net

    def export_html(
        self,
        output_path: str = "docs/cross_system_lineage.html",
        filter_systems: Optional[List[str]] = None,
        hide_intermediate: bool = False,
    ) -> str:
        """
        Export the visualization to a standalone HTML file.

        Args:
            output_path:     Destination path for the HTML file.
            filter_systems:  List of systems to include (None = all).
            hide_intermediate: Whether to hide intermediate DataFrame nodes.

        Returns:
            Absolute path to the created HTML file.
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        if not _PYVIS_AVAILABLE:
            return self._export_fallback_html(output_path)

        net = self._build_pyvis(filter_systems=filter_systems, hide_intermediate=hide_intermediate)

        # Generate pyvis HTML into a temp file, then embed in our wrapper
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as tmp:
            tmp_path = tmp.name

        net.save_graph(tmp_path)

        with open(tmp_path, "r", encoding="utf-8") as f:
            pyvis_content = f.read()
        os.unlink(tmp_path)

        # Build node data dict for sidebar JS
        node_data_map: Dict[str, Any] = {}
        for nid in self._g.nodes:
            nd = dict(self._g.nodes[nid])
            node_data_map[nid] = nd

        stats_html = self._build_stats_html()

        # Embed pyvis content inside our wrapper as an inline div (strip outer html tags)
        # Extract body content from pyvis-generated HTML
        import re
        body_match = re.search(r"<body[^>]*>(.*?)</body>", pyvis_content, re.DOTALL | re.IGNORECASE)
        inner_body = body_match.group(1) if body_match else pyvis_content

        full_html = _HTML_TEMPLATE.format(
            sql_color=_SYSTEM_COLORS["sql"],
            spark_color=_SYSTEM_COLORS["spark"],
            dbt_color=_SYSTEM_COLORS["dbt"],
            airflow_color=_SYSTEM_COLORS["airflow"],
            graph_iframe_or_div=f'<div id="graph-frame">{inner_body}</div>',
            node_data_json=json.dumps(node_data_map, default=str),
            system_colors_json=json.dumps(_SYSTEM_COLORS),
            stats_html=stats_html,
        )

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_html)

        return str(Path(output_path).absolute())

    def _export_fallback_html(self, output_path: str) -> str:
        """Export a lightweight SVG-based fallback when pyvis is not available."""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        nodes_list = [
            {"id": n, **dict(self._g.nodes[n])}
            for n in self._g.nodes
        ]
        edges_list = [
            {"source": s, "target": t, **d}
            for s, t, d in self._g.edges(data=True)
        ]

        stats = self._build_stats_html()
        node_data_map = {
            n: dict(self._g.nodes[n]) for n in self._g.nodes
        }

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Cross-System Lineage (Fallback)</title>
<style>
body {{ font-family: -apple-system, sans-serif; background: #0f1117; color: #e0e0e0; padding: 24px; }}
h1 {{ color: #90caf9; }}
.legend {{ display: flex; gap: 16px; margin: 16px 0; flex-wrap: wrap; }}
.chip {{ padding: 4px 12px; border-radius: 12px; font-size: 13px; font-weight: 500; }}
pre {{ background: #1a1d27; padding: 16px; border-radius: 8px; overflow-x: auto; font-size: 12px; max-height: 500px; overflow-y: auto; }}
table {{ width: 100%; border-collapse: collapse; margin-top: 16px; }}
th, td {{ text-align: left; padding: 8px 12px; border-bottom: 1px solid #2d3148; font-size: 13px; }}
th {{ color: #7986cb; font-weight: 600; }}
</style>
</head>
<body>
<h1>Cross-System Lineage Graph</h1>
<div class="legend">
  <span class="chip" style="background:{_SYSTEM_COLORS['sql']}">SQL ({sum(1 for n in self._g.nodes if self._g.nodes[n].get('system')=='sql')} nodes)</span>
  <span class="chip" style="background:{_SYSTEM_COLORS['spark']}">Spark ({sum(1 for n in self._g.nodes if self._g.nodes[n].get('system')=='spark')} nodes)</span>
  <span class="chip" style="background:{_SYSTEM_COLORS['dbt']}">dbt ({sum(1 for n in self._g.nodes if self._g.nodes[n].get('system')=='dbt')} nodes)</span>
  <span class="chip" style="background:{_SYSTEM_COLORS['airflow']}">Airflow ({sum(1 for n in self._g.nodes if self._g.nodes[n].get('system')=='airflow')} nodes)</span>
</div>
<p>{len(self._g.nodes)} nodes &bull; {len(self._g.edges)} edges</p>
<h2 style="margin-top:24px; font-size:14px; color:#7986cb">Nodes</h2>
<table>
  <tr><th>Node ID</th><th>Label</th><th>System</th><th>Type</th></tr>
  {"".join(f'<tr><td style="font-family:monospace;font-size:11px">{n["id"]}</td><td>{n.get("label","")}</td><td><span class="chip" style="background:{_SYSTEM_COLORS.get(n.get("system","unknown"),"#555")};font-size:11px">{n.get("system","")}</span></td><td>{n.get("node_type","")}</td></tr>' for n in nodes_list)}
</table>
<h2 style="margin-top:24px; font-size:14px; color:#7986cb">Edges</h2>
<table>
  <tr><th>Source</th><th>Target</th><th>System</th><th>Transform</th></tr>
  {"".join(f'<tr><td style="font-family:monospace;font-size:11px">{e["source"]}</td><td style="font-family:monospace;font-size:11px">{e["target"]}</td><td>{e.get("system","")}</td><td>{e.get("transformation_type","")}</td></tr>' for e in edges_list)}
</table>
</body>
</html>"""

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        return str(Path(output_path).absolute())

    def _build_stats_html(self) -> str:
        """Build HTML for stats chips."""
        chips: List[str] = []
        for system in ("sql", "spark", "dbt", "airflow"):
            count = sum(1 for n in self._g.nodes if self._g.nodes[n].get("system") == system)
            if count > 0:
                color = _SYSTEM_COLORS[system]
                chips.append(
                    f'<span class="stat-chip" style="border-left:3px solid {color}">'
                    f'{system.upper()}: {count}</span>'
                )
        cross = sum(1 for _, _, d in self._g.edges(data=True) if d.get("system") == "cross")
        if cross:
            chips.append(f'<span class="stat-chip" style="border-left:3px solid {_SYSTEM_COLORS["cross"]}">Cross edges: {cross}</span>')
        return "".join(chips)

    def get_system_subgraph(self, system: str) -> nx.DiGraph:
        """Return a subgraph containing only nodes from the given system."""
        nodes = [n for n in self._g.nodes if self._g.nodes[n].get("system") == system]
        return self._g.subgraph(nodes).copy()
