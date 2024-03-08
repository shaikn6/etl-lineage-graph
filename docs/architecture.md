# Architecture

## Overview

`etl-lineage-graph` is a four-layer system:

```
SQL Files / API Input
        │
        ▼
 ┌──────────────┐
 │  parser.py   │  sqlparse → extract target, sources, column mappings
 └──────┬───────┘
        │ LineageNode objects
        ▼
 ┌──────────────┐
 │   graph.py   │  networkx DiGraph — nodes = tables, edges = data flow
 └──────┬───────┘
        │
   ┌────┴────┐
   │         │
   ▼         ▼
store.py   report.py
(PostgreSQL) (Mermaid / HTML)
   │
   ▼
 api.py (FastAPI) — REST endpoints
```

## Data Model

### Nodes (tables)

| Attribute | Type | Description |
|-----------|------|-------------|
| table_name | string | Unique table identifier (may include schema prefix) |
| schema | string | Optional schema name |
| row_count_estimate | int | Estimated row count (set externally or None) |
| last_updated | ISO datetime | When this node was last ingested |
| is_source | bool | True if the table has no incoming edges |

### Edges (data flow)

| Attribute | Type | Description |
|-----------|------|-------------|
| source | string | Upstream table |
| target | string | Downstream table |
| column_mappings | list | `[{target_col, source_expression}]` |
| transformation_type | string | aggregate / join / filter / passthrough |
| pipeline_name | string | Optional pipeline identifier |

## SQL Parsing Strategy

`parser.py` uses `sqlparse` to tokenise SQL without executing it:

1. Identify statement type: `INSERT INTO` or `CREATE TABLE AS`
2. Extract target table from the DML/DDL clause
3. Walk `FROM` and `JOIN` tokens to collect source tables
4. Alias map: resolve table aliases to real names
5. Analyse `SELECT` list to produce `{target_col: source_expression}` pairs
6. Strip CTE names (they're virtual, not real source tables)
7. Classify transformation type from keyword presence

## Graph Queries

All queries are powered by `networkx`:

| Query | networkx primitive |
|-------|-------------------|
| `get_upstream` | `nx.ancestors()` |
| `get_downstream` | `nx.descendants()` |
| `get_impact_analysis` | `nx.descendants()` + `nx.shortest_path()` |
| `topological_order` | `nx.topological_sort()` |

## API Design

```
GET  /health               — liveness + node/edge count
POST /parse                — parse SQL string, update graph
GET  /lineage/{table}      — upstream + downstream JSON
GET  /upstream/{table}     — ancestors only
GET  /downstream/{table}   — descendants only
GET  /impact/{table}       — impact analysis with critical paths
GET  /graph                — full graph as JSON
GET  /tables               — list all tracked tables
GET  /mermaid              — Mermaid diagram source
GET  /report               — HTML report (Mermaid + tables)
```

## Persistence

`store.py` uses SQLAlchemy 1.4 (non-async) with `psycopg2`. The two tables
`lineage_nodes` and `lineage_edges` are created on startup via `Base.metadata.create_all`.

Upsert strategy:
- Nodes: `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL dialect)
- Edges: DELETE + INSERT (simplest approach for column mapping updates)

The in-memory `LineageGraph` is the primary data structure during a session.
`persist_graph()` snapshots the full graph to PostgreSQL for durability.
