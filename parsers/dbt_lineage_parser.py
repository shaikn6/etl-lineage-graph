"""
dbt lineage parser: Extract model dependency graph from dbt YAML + SQL.

Handles:
- ref() macro → inter-model dependencies
- source() macro → source table references
- Column-level lineage from SELECT statements in dbt SQL models
- dbt model → underlying warehouse table mapping
- Exposure and test YAML discovery
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class DbtColumn:
    """Column-level lineage within a dbt model."""
    target_col: str
    source_expression: str
    source_model: Optional[str] = None  # resolved ref() name if derivable


@dataclass
class DbtModelNode:
    """
    Lineage information for a single dbt model.

    Attributes:
        model_name:       dbt model name (= SQL file stem)
        warehouse_table:  fully qualified warehouse table this model materialises to
        ref_deps:         model names referenced via ref(...)
        source_deps:      (source_name, table_name) tuples from source(...)
        column_lineage:   column-level SELECT mappings
        raw_sql:          original dbt SQL / Jinja template
        materialization:  table | view | incremental | ephemeral
        tags:             list of dbt tags
        description:      model description from schema.yml
    """
    model_name: str
    warehouse_table: str
    ref_deps: List[str] = field(default_factory=list)
    source_deps: List[Tuple[str, str]] = field(default_factory=list)
    column_lineage: List[DbtColumn] = field(default_factory=list)
    raw_sql: str = ""
    materialization: str = "view"
    tags: List[str] = field(default_factory=list)
    description: str = ""

    @property
    def all_upstream(self) -> List[str]:
        """All upstream identifiers: ref models + source table names."""
        upstream = list(self.ref_deps)
        for src_name, tbl_name in self.source_deps:
            upstream.append(f"{src_name}.{tbl_name}")
        return upstream


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Match {{ ref('model_name') }} or {{ ref("model_name") }}
_REF_PATTERN = re.compile(r"""\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}""", re.IGNORECASE)

# Match {{ source('source_name', 'table_name') }}
_SOURCE_PATTERN = re.compile(
    r"""\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}""",
    re.IGNORECASE,
)

# Match config block: {{ config(materialized='table', ...) }}
_CONFIG_PATTERN = re.compile(
    r"""\{\{\s*config\s*\(([^}]*)\)\s*\}\}""", re.IGNORECASE | re.DOTALL
)

# Match SELECT column list (simplified — handles most common patterns)
_SELECT_PATTERN = re.compile(
    r"""SELECT\s+(.*?)\s+FROM""", re.IGNORECASE | re.DOTALL
)

# Match column alias: expression AS alias
_COL_ALIAS_PATTERN = re.compile(
    r"""(.*?)\s+AS\s+(\w+)\s*$""", re.IGNORECASE
)


# ---------------------------------------------------------------------------
# SQL / Jinja helpers
# ---------------------------------------------------------------------------

def _strip_jinja(sql: str) -> str:
    """Replace Jinja expressions with placeholder identifiers for SQL parsing."""
    # Replace {{ ref(...) }} with the model name so SQL is parseable
    sql = _REF_PATTERN.sub(lambda m: m.group(1), sql)
    # Replace {{ source(...) }} with source_table
    sql = _SOURCE_PATTERN.sub(lambda m: f"{m.group(1)}__{m.group(2)}", sql)
    # Strip remaining {{ ... }} and {% ... %}
    sql = re.sub(r"\{\{[^}]*\}\}", "placeholder", sql)
    sql = re.sub(r"\{%-?.*?-%?\}", "", sql, flags=re.DOTALL)
    return sql


def _extract_refs(sql: str) -> List[str]:
    """Extract all ref() model names from Jinja SQL."""
    return _REF_PATTERN.findall(sql)


def _extract_sources(sql: str) -> List[Tuple[str, str]]:
    """Extract all source(src, table) pairs from Jinja SQL."""
    return _SOURCE_PATTERN.findall(sql)


def _extract_materialization(sql: str) -> str:
    """Extract materialization type from config block."""
    config_match = _CONFIG_PATTERN.search(sql)
    if not config_match:
        return "view"
    config_body = config_match.group(1)
    mat_match = re.search(
        r"""materialized\s*=\s*['"]([^'"]+)['"]""", config_body, re.IGNORECASE
    )
    return mat_match.group(1) if mat_match else "view"


def _extract_column_lineage(sql: str, model_name: str) -> List[DbtColumn]:
    """
    Extract simple column-level lineage from the outermost SELECT statement.

    Handles:
    - col_name (passthrough)
    - expression AS alias
    - table.col (prefixed columns)
    Skips * selects, subqueries, and complex CTEs.
    """
    # Strip Jinja before SQL parsing
    clean_sql = _strip_jinja(sql)

    # Find the final / outermost SELECT...FROM block
    select_match = None
    for m in _SELECT_PATTERN.finditer(clean_sql):
        select_match = m  # keep last match (outermost in many patterns)

    if not select_match:
        return []

    col_clause = select_match.group(1).strip()

    # Split on commas not inside parentheses
    cols: List[str] = []
    depth = 0
    current: List[str] = []
    for ch in col_clause:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            cols.append("".join(current).strip())
            current = []
            continue
        current.append(ch)
    if current:
        cols.append("".join(current).strip())

    lineage: List[DbtColumn] = []
    for col in cols:
        col = col.strip()
        if not col or col == "*":
            continue

        alias_match = _COL_ALIAS_PATTERN.match(col)
        if alias_match:
            expr = alias_match.group(1).strip()
            alias = alias_match.group(2).strip()
            lineage.append(DbtColumn(target_col=alias, source_expression=expr))
        else:
            # Might be table.col or just col
            parts = col.split(".")
            col_name = parts[-1].strip()
            lineage.append(DbtColumn(target_col=col_name, source_expression=col))

    return lineage


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_dbt_model(
    sql: str,
    model_name: str,
    database: str = "analytics",
    schema: str = "public",
    description: str = "",
    tags: Optional[List[str]] = None,
) -> DbtModelNode:
    """
    Parse a single dbt model SQL file and return its lineage node.

    Args:
        sql:          Raw dbt SQL/Jinja template string.
        model_name:   Model name (typically the file stem, e.g. "orders_fact").
        database:     Target database (default: analytics).
        schema:       Target schema (default: public).
        description:  Model description from schema.yml.
        tags:         Model tags from schema.yml.

    Returns:
        DbtModelNode with full dependency and column-level lineage.
    """
    ref_deps = _extract_refs(sql)
    source_deps = _extract_sources(sql)
    materialization = _extract_materialization(sql)
    column_lineage = _extract_column_lineage(sql, model_name)
    warehouse_table = f"{database}.{schema}.{model_name}"

    return DbtModelNode(
        model_name=model_name,
        warehouse_table=warehouse_table,
        ref_deps=ref_deps,
        source_deps=source_deps,
        column_lineage=column_lineage,
        raw_sql=sql,
        materialization=materialization,
        tags=tags or [],
        description=description,
    )


def parse_dbt_project(
    models_dir: str,
    database: str = "analytics",
    schema: str = "public",
) -> List[DbtModelNode]:
    """
    Walk a dbt models/ directory and parse all .sql files.

    Args:
        models_dir:  Path to the dbt models/ directory.
        database:    Target database for warehouse_table mapping.
        schema:      Target schema for warehouse_table mapping.

    Returns:
        List of DbtModelNode, one per .sql file found.
    """
    nodes: List[DbtModelNode] = []
    models_path = Path(models_dir)

    if not models_path.exists():
        return nodes

    # Load schema.yml descriptions if present
    descriptions: Dict[str, str] = {}
    tags_map: Dict[str, List[str]] = {}
    schema_files = list(models_path.rglob("schema.yml")) + list(models_path.rglob("*.yml"))
    for schema_file in schema_files:
        _parse_schema_yaml(schema_file, descriptions, tags_map)

    for sql_file in models_path.rglob("*.sql"):
        model_name = sql_file.stem
        sql = sql_file.read_text(encoding="utf-8")
        node = parse_dbt_model(
            sql=sql,
            model_name=model_name,
            database=database,
            schema=schema,
            description=descriptions.get(model_name, ""),
            tags=tags_map.get(model_name, []),
        )
        nodes.append(node)

    return nodes


def _parse_schema_yaml(
    schema_file: Path,
    descriptions: Dict[str, str],
    tags_map: Dict[str, List[str]],
) -> None:
    """
    Extract model descriptions and tags from a dbt schema.yml file.
    Uses simple regex parsing to avoid yaml dependency requirements.
    """
    content = schema_file.read_text(encoding="utf-8")

    # Find model blocks: - name: model_name
    model_blocks = re.split(r"(?=^\s*-\s+name:\s+\w+)", content, flags=re.MULTILINE)
    for block in model_blocks:
        name_match = re.search(r"^\s*-\s+name:\s+(\w+)", block, re.MULTILINE)
        if not name_match:
            continue
        model_name = name_match.group(1)

        desc_match = re.search(r"description:\s*['\"]?([^'\"\n]+)['\"]?", block)
        if desc_match:
            descriptions[model_name] = desc_match.group(1).strip()

        tags_match = re.search(r"tags:\s*\[([^\]]+)\]", block)
        if tags_match:
            raw_tags = tags_match.group(1)
            tags = [t.strip().strip("'\"") for t in raw_tags.split(",") if t.strip()]
            tags_map[model_name] = tags


def build_dbt_dependency_graph(nodes: List[DbtModelNode]) -> Dict[str, List[str]]:
    """
    Build a simple adjacency dict: model_name → [upstream model_names].

    Useful for rendering or passing to the cross-system merger.
    """
    graph: Dict[str, List[str]] = {}
    for node in nodes:
        graph[node.model_name] = list(node.ref_deps)
    return graph
