# Changelog

## v2.0.0 — 2026-05-30

### What's New

- **Cross-system lineage**: unified graph spanning SQL + PySpark + dbt + Airflow via `graph/cross_system_merger.py`
- **Spark lineage parser**: AST-based extraction from PySpark source code — handles `spark.read.parquet`, `df.write.parquet`, `.join()`, `.select()`, `.groupBy()`, `.filter()`, `.union()`, and multi-step pipelines (`parsers/spark_lineage_parser.py`)
- **dbt lineage parser**: `ref()` and `source()` macro resolution, column-level lineage from SELECT statements, materialization extraction, warehouse table mapping, schema.yml description/tag ingestion (`parsers/dbt_lineage_parser.py`)
- **Change impact propagation**: BFS traversal of the unified graph with BREAKING / WARNING / OK severity classification, per-system blast radius counts, and time-to-fix estimates (`impact/propagation_simulator.py`)
- **Enhanced visualization**: color-coded nodes by system (SQL=blue, Spark=orange, dbt=green, Airflow=purple), click-to-see-code sidebar, system filter controls, cross-system edge highlighting (`viz/cross_system_viz.py`)
- **V2 dashboard**: 4-tab Streamlit app — SQL Lineage (V1), Cross-System Graph, Impact Analysis, System Comparison (`dashboard/app_v2.py`)
- **4 new PNGs**: `docs/screenshots/spark_ast_lineage.png`, `dbt_lineage.png`, `cross_system_lineage.png`, `impact_propagation.png`

### Improvements

- Lineage coverage expanded from SQL-only (V1) to 4 systems: SQL, Spark, dbt, Airflow
- Impact analysis now traverses cross-system edges (Airflow → Spark → dbt paths)
- Cross-system edge detection via shared path/table name index — normalized, case-insensitive matching
- `CrossSystemMerger` supports incremental ingestion and `system_coverage_stats()` reporting

### Under the Hood

- `+88 tests` covering Spark AST parsing (20), dbt model parsing (20), cross-system merging (19), and impact propagation (29) — total test suite: 140 tests, all passing
- Spark write detection fixed: walks `Attribute.value` chain to find `.write` preceding format method calls
- Pure Python — no PySpark runtime required for static AST analysis
- pyvis optional: falls back to lightweight HTML table export when not installed

---

## v1.0.0 — 2026-05-30

- Column-level SQL lineage via CTE-aware parser + interactive networkx/Mermaid graph
- FastAPI REST API: `/parse`, `/lineage/{table}`, `/impact/{table}`, `/graph`
- PostgreSQL persistence via SQLAlchemy
- HTML lineage report with Mermaid diagram
- Docker + CI/CD via GitHub Actions
- 52 tests covering SQL parsing, graph construction, impact analysis, and report generation
