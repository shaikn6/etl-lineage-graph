"""
Spark lineage parser: AST-based extraction of PySpark data lineage.

Extracts:
- Source datasets (spark.read.parquet / csv / json / table)
- Sink datasets (df.write.parquet / csv / json / saveAsTable)
- Transformations: join, select, groupBy, filter, union, withColumn
- Multi-step pipelines via intermediate DataFrame variable tracking
"""

from __future__ import annotations

import ast
import textwrap
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class SparkDataset:
    """A single Spark dataset node (source or sink)."""

    path: str
    format: str  # parquet | csv | json | table | unknown
    dataset_type: str  # source | sink | intermediate


@dataclass
class SparkTransformation:
    """A transformation step applied to a DataFrame variable."""

    operation: str  # join | select | groupBy | filter | union | withColumn
    input_vars: List[str] = field(default_factory=list)
    output_var: str = ""
    details: Dict = field(default_factory=dict)


@dataclass
class SparkLineageNode:
    """Complete lineage for a single PySpark pipeline / script."""

    pipeline_name: str
    sources: List[SparkDataset]
    sinks: List[SparkDataset]
    transformations: List[SparkTransformation]
    intermediate_vars: List[str]
    raw_code: str

    # Flattened convenience lists
    @property
    def source_paths(self) -> List[str]:
        return [s.path for s in self.sources]

    @property
    def sink_paths(self) -> List[str]:
        return [s.path for s in self.sinks]


# ---------------------------------------------------------------------------
# AST visitor
# ---------------------------------------------------------------------------

_READ_FORMATS = {"parquet", "csv", "json", "orc", "avro", "text"}
_WRITE_FORMATS = {"parquet", "csv", "json", "orc", "avro", "text", "saveastable"}
_TRANSFORM_OPS = {
    "join",
    "select",
    "groupby",
    "filter",
    "where",
    "union",
    "unionall",
    "unionbyname",
    "withcolumn",
    "drop",
    "distinct",
    "limit",
    "orderby",
    "sortby",
    "agg",
    "aggregate",
}


class _SparkVisitor(ast.NodeVisitor):
    """Walk a PySpark AST and collect read/write/transform calls."""

    def __init__(self, pipeline_name: str, raw_code: str) -> None:
        self.pipeline_name = pipeline_name
        self.raw_code = raw_code

        # var_name → SparkDataset (for read-assigned variables)
        self._var_datasets: Dict[str, SparkDataset] = {}
        # var_name → origin var_names (tracks DataFrame lineage)
        self._var_origins: Dict[str, List[str]] = {}

        self.sources: List[SparkDataset] = []
        self.sinks: List[SparkDataset] = []
        self.transformations: List[SparkTransformation] = []

        # Track intermediate vars (assigned, not sources/sinks)
        self._intermediate_vars: Set[str] = set()
        self._source_vars: Set[str] = set()
        self._sink_vars: Set[str] = set()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_string_arg(self, node: ast.Call, pos: int = 0) -> Optional[str]:
        """Extract positional string literal from a Call node."""
        if pos < len(node.args):
            arg = node.args[pos]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                return arg.value
        # Also check keyword args (path=, name=, tableName=)
        for kw in node.keywords:
            if kw.arg in ("path", "name", "tableName", "table"):
                if isinstance(kw.value, ast.Constant):
                    return str(kw.value.value)
        return None

    def _extract_var_name(self, node: ast.expr) -> Optional[str]:
        """Get simple variable name from a Name node."""
        if isinstance(node, ast.Name):
            return node.id
        return None

    def _chain_root_var(self, node: ast.expr) -> Optional[str]:
        """Walk a method-call chain and return the root variable name."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Call):
            return self._chain_root_var(node.func)
        if isinstance(node, ast.Attribute):
            return self._chain_root_var(node.value)
        return None

    def _collect_method_chain(self, node: ast.expr) -> List[str]:
        """Return list of method names in a call chain (innermost first)."""
        methods: List[str] = []
        current = node
        while isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Attribute):
                methods.append(func.attr.lower())
            current = func
            if isinstance(current, ast.Attribute):
                current = current.value
        return methods

    # ------------------------------------------------------------------
    # Read detection: spark.read.<format>(...) or spark.read.load(...)
    # ------------------------------------------------------------------

    def _is_read_call(self, node: ast.Call) -> Tuple[bool, str, str]:
        """Return (is_read, path, format)."""
        func = node.func
        if not isinstance(func, ast.Attribute):
            return False, "", ""

        method = func.attr.lower()
        parent = func.value

        # spark.read.parquet("path") / spark.read.csv("path") etc.
        if method in _READ_FORMATS:
            path = self._extract_string_arg(node) or f"<unknown_{method}>"
            return True, path, method

        # spark.read.load("path", format="parquet")
        if method == "load":
            path = self._extract_string_arg(node) or "<unknown_load>"
            fmt = "unknown"
            for kw in node.keywords:
                if kw.arg == "format" and isinstance(kw.value, ast.Constant):
                    fmt = kw.value.value
            return True, path, fmt

        # spark.table("table_name") or spark.sql(...)
        if (
            method == "table"
            and isinstance(parent, ast.Name)
            and parent.id in ("spark", "sqlContext", "sc")
        ):
            path = self._extract_string_arg(node) or "<unknown_table>"
            return True, path, "table"

        return False, "", ""

    # ------------------------------------------------------------------
    # Write detection: df.write.parquet("path") / df.write.saveAsTable("t")
    # ------------------------------------------------------------------

    def _is_write_call(self, node: ast.Call) -> Tuple[bool, str, str]:
        """Return (is_write, path, format)."""
        func = node.func
        if not isinstance(func, ast.Attribute):
            return False, "", ""

        method = func.attr.lower()

        if method in _WRITE_FORMATS:
            path = self._extract_string_arg(node) or f"<unknown_{method}>"
            return True, path, method if method != "saveastable" else "table"

        if method == "save":
            path = self._extract_string_arg(node) or "<unknown_save>"
            fmt = "unknown"
            for kw in node.keywords:
                if kw.arg == "format" and isinstance(kw.value, ast.Constant):
                    fmt = kw.value.value
            return True, path, fmt

        return False, "", ""

    def _has_write_ancestor(self, node: ast.Call) -> bool:
        """
        Check if .write is somewhere in the attribute chain above this call.

        df.write.parquet("path") → func = Attribute(value=Attribute(value=Name('df'), attr='write'), attr='parquet')
        We need to walk the Attribute.value chain, not just Call.func chains.
        """
        func = node.func
        if not isinstance(func, ast.Attribute):
            return False

        # Walk up through Attribute.value nodes looking for attr='write'
        current = func
        while isinstance(current, ast.Attribute):
            if current.attr.lower() == "write":
                return True
            current = current.value
        return False

    # ------------------------------------------------------------------
    # Transformation detection
    # ------------------------------------------------------------------

    def _detect_transformation(
        self, call_node: ast.Call
    ) -> Optional[SparkTransformation]:
        func = call_node.func
        if not isinstance(func, ast.Attribute):
            return None

        op = func.attr.lower()
        if op not in _TRANSFORM_OPS:
            return None

        root_var = self._chain_root_var(func.value)
        details: Dict = {}

        if op == "join":
            # df1.join(df2, on=..., how=...) → inputs = [df1, df2]
            right_var = (
                self._extract_var_name(call_node.args[0]) if call_node.args else None
            )
            left_var = root_var
            inputs = [v for v in [left_var, right_var] if v]
            for kw in call_node.keywords:
                if kw.arg == "how" and isinstance(kw.value, ast.Constant):
                    details["how"] = kw.value.value
            return SparkTransformation(
                operation="join", input_vars=inputs, details=details
            )

        if op in ("union", "unionall", "unionbyname"):
            right_var = (
                self._extract_var_name(call_node.args[0]) if call_node.args else None
            )
            left_var = root_var
            inputs = [v for v in [left_var, right_var] if v]
            return SparkTransformation(
                operation="union", input_vars=inputs, details=details
            )

        if op == "select":
            cols = []
            for arg in call_node.args:
                if isinstance(arg, ast.Constant):
                    cols.append(arg.value)
            details["columns"] = cols
            return SparkTransformation(
                operation="select",
                input_vars=[root_var] if root_var else [],
                details=details,
            )

        if op == "groupby":
            cols = []
            for arg in call_node.args:
                if isinstance(arg, ast.Constant):
                    cols.append(arg.value)
            details["group_by"] = cols
            return SparkTransformation(
                operation="groupBy",
                input_vars=[root_var] if root_var else [],
                details=details,
            )

        if op in ("filter", "where"):
            return SparkTransformation(
                operation="filter",
                input_vars=[root_var] if root_var else [],
                details=details,
            )

        if op == "withcolumn":
            col_name = self._extract_string_arg(call_node) or "<col>"
            details["column"] = col_name
            return SparkTransformation(
                operation="withColumn",
                input_vars=[root_var] if root_var else [],
                details=details,
            )

        return SparkTransformation(
            operation=op, input_vars=[root_var] if root_var else [], details=details
        )

    # ------------------------------------------------------------------
    # AST visitor methods
    # ------------------------------------------------------------------

    def visit_Assign(self, node: ast.Assign) -> None:
        """Handle variable assignments: detect reads and transform chains."""
        if not node.targets:
            self.generic_visit(node)
            return

        # Single-target assignment only
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            self.generic_visit(node)
            return

        var_name = target.id
        value = node.value

        # Case 1: Direct read assignment — df = spark.read.parquet("path")
        if isinstance(value, ast.Call):
            is_read, path, fmt = self._is_read_call(value)
            if is_read:
                ds = SparkDataset(path=path, format=fmt, dataset_type="source")
                self.sources.append(ds)
                self._var_datasets[var_name] = ds
                self._source_vars.add(var_name)
                self.generic_visit(node)
                return

            # Case 2: Transform chain — df2 = df1.filter(...).select(...)
            root_var = self._chain_root_var(value)
            if (
                root_var
                and root_var in self._var_datasets
                or root_var in self._intermediate_vars
                or root_var in self._source_vars
            ):
                self._intermediate_vars.add(var_name)
                # Track chain origins
                origins = self._var_origins.get(root_var, [root_var])
                self._var_origins[var_name] = origins

                # Extract all transformations in chain
                self._extract_transforms_from_chain(value, output_var=var_name)
                self.generic_visit(node)
                return

            # Check join / union (two-var ops)
            if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
                op = value.func.attr.lower()
                if op in ("join", "union", "unionall", "unionbyname"):
                    t = self._detect_transformation(value)
                    if t:
                        t.output_var = var_name
                        self.transformations.append(t)
                        self._intermediate_vars.add(var_name)
                        self.generic_visit(node)
                        return

        self.generic_visit(node)

    def _extract_transforms_from_chain(
        self, node: ast.Call, output_var: str = ""
    ) -> None:
        """Recursively walk a call chain and emit SparkTransformation objects."""
        t = self._detect_transformation(node)
        if t:
            t.output_var = output_var
            self.transformations.append(t)

        # Recurse into the chained receiver
        if isinstance(node.func, ast.Attribute) and isinstance(
            node.func.value, ast.Call
        ):
            self._extract_transforms_from_chain(node.func.value, output_var="")

    def visit_Expr(self, node: ast.Expr) -> None:
        """Handle standalone expression statements: df.write.parquet(...)."""
        if isinstance(node.value, ast.Call):
            call = node.value
            if self._has_write_ancestor(call):
                is_write, path, fmt = self._is_write_call(call)
                if is_write:
                    ds = SparkDataset(path=path, format=fmt, dataset_type="sink")
                    self.sinks.append(ds)
                    root_var = self._chain_root_var(call)
                    if root_var:
                        self._sink_vars.add(root_var)
        self.generic_visit(node)

    def build(self) -> SparkLineageNode:
        intermediate = list(
            self._intermediate_vars - self._source_vars - self._sink_vars
        )
        return SparkLineageNode(
            pipeline_name=self.pipeline_name,
            sources=self.sources,
            sinks=self.sinks,
            transformations=self.transformations,
            intermediate_vars=intermediate,
            raw_code=self.raw_code,
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_spark_code(
    code: str, pipeline_name: str = "spark_pipeline"
) -> SparkLineageNode:
    """
    Parse PySpark source code and return a SparkLineageNode.

    Args:
        code: PySpark Python source code string.
        pipeline_name: Logical name for this pipeline (used in graph nodes).

    Returns:
        SparkLineageNode with sources, sinks, and transformations.

    Raises:
        SyntaxError: If the code cannot be parsed as Python.
    """
    code = textwrap.dedent(code)
    tree = ast.parse(code)
    visitor = _SparkVisitor(pipeline_name=pipeline_name, raw_code=code)
    visitor.visit(tree)
    return visitor.build()


def parse_spark_file(
    filepath: str, pipeline_name: Optional[str] = None
) -> SparkLineageNode:
    """
    Parse a .py PySpark file and return its lineage.

    Args:
        filepath: Absolute path to the .py file.
        pipeline_name: Override pipeline name (default: filename stem).

    Returns:
        SparkLineageNode.
    """
    import pathlib

    p = pathlib.Path(filepath)
    if pipeline_name is None:
        pipeline_name = p.stem
    code = p.read_text(encoding="utf-8")
    return parse_spark_code(code, pipeline_name=pipeline_name)
