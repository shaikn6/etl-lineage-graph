"""SQL parser: extracts source/target tables and column-level mappings from ETL SQL statements."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis
from sqlparse.tokens import CTE, DML, Keyword


@dataclass
class ColumnMapping:
    target_col: str
    source_expression: str
    source_table: Optional[str] = None


@dataclass
class LineageNode:
    target_table: str
    source_tables: List[str]
    column_mappings: List[ColumnMapping]
    transformation_type: str  # 'aggregate' | 'join' | 'filter' | 'passthrough'
    raw_sql: str
    pipeline_name: Optional[str] = None
    cte_names: List[str] = field(default_factory=list)


def _clean_identifier(name: str) -> str:
    """Strip quotes and schema prefix alias from a raw identifier string."""
    name = name.strip().strip('"').strip("'").strip("`")
    return name


def _resolve_alias(identifier: Identifier) -> Tuple[str, Optional[str]]:
    """Return (real_name, alias) for an Identifier node."""
    alias = identifier.get_alias()
    real_name = identifier.get_real_name()
    return (_clean_identifier(real_name) if real_name else ""), alias


def _extract_cte_names(statement) -> List[str]:
    """Pull CTE names from a WITH ... AS (...) clause (before or after INSERT INTO)."""
    ctes: List[str] = []
    tokens = list(statement.tokens)
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        # sqlparse uses Token.Keyword.CTE for WITH when it follows INSERT INTO,
        # and Token.Keyword for WITH at statement start.
        is_with = (
            tok.ttype is CTE or tok.ttype is Keyword
        ) and tok.normalized.upper() == "WITH"
        if is_with:
            i += 1
            while i < len(tokens):
                t = tokens[i]
                if isinstance(t, Identifier):
                    name = t.get_real_name()
                    if name:
                        ctes.append(_clean_identifier(name))
                elif isinstance(t, IdentifierList):
                    for ident in t.get_identifiers():
                        if isinstance(ident, Identifier):
                            name = ident.get_real_name()
                            if name:
                                ctes.append(_clean_identifier(name))
                elif t.ttype is DML:
                    break
                i += 1
            # Keep scanning — there may be multiple WITH blocks (rare but valid)
        i += 1
    return ctes


def _extract_from_tables(tokens, alias_map: Dict[str, str]) -> List[str]:
    """Walk token list and collect tables listed after FROM and JOIN keywords."""
    tables: List[str] = []
    i = 0
    token_list = list(tokens)
    while i < len(token_list):
        tok = token_list[i]

        # Recurse into parentheses (subqueries and CTE bodies)
        if isinstance(tok, Parenthesis):
            inner_stmt = sqlparse.parse(tok.value[1:-1])
            for inner in inner_stmt:
                inner_tables = _extract_from_tables(inner.tokens, alias_map)
                tables.extend(inner_tables)
            i += 1
            continue

        # Recurse into CTE Identifier nodes — "cte_name AS (<body>)"
        # These have ttype=None and contain a Parenthesis child with the body.
        if isinstance(tok, Identifier) and tok.ttype is None:
            for subtok in tok.tokens:
                if isinstance(subtok, Parenthesis):
                    inner_stmt = sqlparse.parse(subtok.value[1:-1])
                    for inner in inner_stmt:
                        inner_tables = _extract_from_tables(inner.tokens, alias_map)
                        tables.extend(inner_tables)
            i += 1
            continue

        is_from_or_join = tok.ttype is Keyword and tok.normalized.upper() in (
            "FROM",
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
            "CROSS JOIN",
            "LEFT OUTER JOIN",
            "RIGHT OUTER JOIN",
        )

        if is_from_or_join:
            i += 1
            # Skip whitespace
            while i < len(token_list) and token_list[i].ttype in (
                sqlparse.tokens.Whitespace,
                sqlparse.tokens.Newline,
            ):
                i += 1

            if i < len(token_list):
                next_tok = token_list[i]
                if isinstance(next_tok, Identifier):
                    real, alias = _resolve_alias(next_tok)
                    if real:
                        tables.append(real)
                        if alias:
                            alias_map[alias] = real
                elif isinstance(next_tok, IdentifierList):
                    for ident in next_tok.get_identifiers():
                        if isinstance(ident, Identifier):
                            real, alias = _resolve_alias(ident)
                            if real:
                                tables.append(real)
                                if alias:
                                    alias_map[alias] = real
                elif next_tok.ttype not in (None,) and next_tok.value:
                    name = _clean_identifier(next_tok.value)
                    if name and not name.upper() in (
                        "SELECT",
                        "WHERE",
                        "GROUP",
                        "ORDER",
                        "HAVING",
                    ):
                        tables.append(name)
            continue
        i += 1
    return tables


def _extract_target_table(statement) -> Optional[str]:
    """Extract target table from INSERT INTO or CREATE TABLE AS."""
    tokens = list(statement.tokens)
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        val = tok.normalized.upper() if tok.ttype else ""

        # INSERT INTO <table>
        if tok.ttype is DML and val == "INSERT":
            i += 1
            while i < len(tokens) and tokens[i].ttype in (
                sqlparse.tokens.Whitespace,
                sqlparse.tokens.Newline,
                sqlparse.tokens.Keyword,
            ):
                into_tok = tokens[i]
                if into_tok.ttype is Keyword and into_tok.normalized.upper() == "INTO":
                    i += 1
                    break
                i += 1
            while i < len(tokens) and tokens[i].ttype in (
                sqlparse.tokens.Whitespace,
                sqlparse.tokens.Newline,
            ):
                i += 1
            if i < len(tokens):
                target_tok = tokens[i]
                if isinstance(target_tok, Identifier):
                    real, _ = _resolve_alias(target_tok)
                    return real or None
                elif target_tok.ttype not in (None,):
                    return _clean_identifier(target_tok.value) or None
            return None

        # CREATE TABLE <name> AS
        if tok.ttype is sqlparse.tokens.DDL and val == "CREATE":
            i += 1
            while i < len(tokens):
                t = tokens[i]
                if t.ttype is Keyword and t.normalized.upper() == "TABLE":
                    i += 1
                    while i < len(tokens) and tokens[i].ttype in (
                        sqlparse.tokens.Whitespace,
                        sqlparse.tokens.Newline,
                        sqlparse.tokens.Keyword,
                    ):
                        extra = tokens[i]
                        # skip OR REPLACE, IF NOT EXISTS modifiers
                        if extra.ttype is Keyword and extra.normalized.upper() in (
                            "OR",
                            "REPLACE",
                            "IF",
                            "NOT",
                            "EXISTS",
                        ):
                            pass
                        i += 1
                    if i < len(tokens):
                        target_tok = tokens[i]
                        if isinstance(target_tok, Identifier):
                            real, _ = _resolve_alias(target_tok)
                            return real or None
                        else:
                            return _clean_identifier(target_tok.value) or None
                    return None
                i += 1
            return None

        i += 1
    return None


def _extract_select_columns(tokens) -> List[Tuple[str, str]]:
    """Return list of (target_col_name, source_expression) from a SELECT list."""
    pairs: List[Tuple[str, str]] = []
    in_select = False
    token_list = list(tokens)

    for i, tok in enumerate(token_list):
        if tok.ttype is DML and tok.normalized.upper() == "SELECT":
            in_select = True
            continue

        if in_select:
            if tok.ttype is Keyword and tok.normalized.upper() in ("FROM", "INTO"):
                break

            if isinstance(tok, IdentifierList):
                for ident in tok.get_identifiers():
                    if isinstance(ident, Identifier):
                        alias = ident.get_alias()
                        # Strip alias from the right side to get source expr
                        if alias:
                            # source expr is everything left of AS alias
                            src_expr = ident.value
                            # remove trailing "AS alias" or just "alias"
                            src_expr = re.sub(
                                r"\s+(?:AS\s+)?" + re.escape(alias) + r"\s*$",
                                "",
                                src_expr,
                                flags=re.IGNORECASE,
                            ).strip()
                            pairs.append((alias, src_expr))
                        else:
                            real = ident.get_real_name()
                            if real:
                                pairs.append((real, real))
                    elif ident.ttype is sqlparse.tokens.Wildcard:
                        pairs.append(("*", "*"))
                break

            if isinstance(tok, Identifier):
                alias = tok.get_alias()
                if alias:
                    src_expr = tok.value
                    src_expr = re.sub(
                        r"\s+(?:AS\s+)?" + re.escape(alias) + r"\s*$",
                        "",
                        src_expr,
                        flags=re.IGNORECASE,
                    ).strip()
                    pairs.append((alias, src_expr))
                else:
                    real = tok.get_real_name()
                    if real:
                        pairs.append((real, real))
                break

    return pairs


def _detect_transformation_type(statement, source_tables: List[str]) -> str:
    """Classify transformation as aggregate | join | filter | passthrough."""
    sql_upper = statement.value.upper()
    has_aggregate = any(
        kw in sql_upper for kw in ("GROUP BY", "SUM(", "COUNT(", "AVG(", "MAX(", "MIN(")
    )
    has_join = len(source_tables) > 1 or any(
        kw in sql_upper for kw in (" JOIN ", "INNER JOIN", "LEFT JOIN")
    )
    has_filter = "WHERE" in sql_upper

    if has_aggregate:
        return "aggregate"
    if has_join:
        return "join"
    if has_filter:
        return "filter"
    return "passthrough"


def parse_sql(sql: str, pipeline_name: Optional[str] = None) -> List[LineageNode]:
    """
    Parse one or more SQL statements and return a list of LineageNode objects.

    Each node captures:
    - target_table: the table being written
    - source_tables: tables read from
    - column_mappings: target → source expression pairs
    - transformation_type: aggregate | join | filter | passthrough
    """
    nodes: List[LineageNode] = []
    statements = sqlparse.parse(sql.strip())

    for stmt in statements:
        if not stmt.tokens:
            continue

        # Skip blank / comment-only tokens. sqlparse sometimes wraps multiple
        # comment lines into a compound token with ttype=None — detect those
        # by flattening and checking if all leaf tokens are comments/whitespace.
        def _is_comment_or_ws(tok) -> bool:
            if tok.ttype in (
                sqlparse.tokens.Whitespace,
                sqlparse.tokens.Newline,
                sqlparse.tokens.Comment.Single,
                sqlparse.tokens.Comment.Multiline,
            ):
                return True
            if tok.ttype is None:
                # compound token — check leaves
                leaves = list(tok.flatten())
                return all(
                    t.ttype
                    in (
                        sqlparse.tokens.Whitespace,
                        sqlparse.tokens.Newline,
                        sqlparse.tokens.Comment.Single,
                        sqlparse.tokens.Comment.Multiline,
                    )
                    for t in leaves
                )
            return False

        non_ws = [
            t for t in stmt.tokens if not _is_comment_or_ws(t) and t.value.strip()
        ]
        if not non_ws:
            continue

        first = non_ws[0]
        stmt_type = ""
        if first.ttype is DML:
            stmt_type = first.normalized.upper()
        elif first.ttype is sqlparse.tokens.DDL:
            stmt_type = first.normalized.upper()
        elif first.ttype is Keyword and first.normalized.upper() == "WITH":
            stmt_type = "WITH"
        elif first.ttype is CTE:
            stmt_type = "WITH"

        if stmt_type not in ("INSERT", "CREATE", "WITH"):
            continue

        cte_names = _extract_cte_names(stmt)
        alias_map: Dict[str, str] = {}
        target_table = _extract_target_table(stmt)
        if not target_table:
            continue

        all_from_tables = _extract_from_tables(stmt.tokens, alias_map)

        # Remove CTEs (they are virtual, not real sources) and self-references
        source_tables = [
            t for t in all_from_tables if t not in cte_names and t != target_table
        ]
        # Deduplicate preserving order
        seen: set = set()
        deduped: List[str] = []
        for t in source_tables:
            key = t.lower()
            if key not in seen:
                seen.add(key)
                deduped.append(t)
        source_tables = deduped

        col_pairs = _extract_select_columns(stmt.tokens)
        column_mappings = [
            ColumnMapping(target_col=tc, source_expression=src) for tc, src in col_pairs
        ]

        transformation_type = _detect_transformation_type(stmt, source_tables)

        nodes.append(
            LineageNode(
                target_table=target_table,
                source_tables=source_tables,
                column_mappings=column_mappings,
                transformation_type=transformation_type,
                raw_sql=stmt.value.strip(),
                pipeline_name=pipeline_name,
                cte_names=cte_names,
            )
        )

    return nodes
