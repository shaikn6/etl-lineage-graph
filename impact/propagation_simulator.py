"""
Change impact propagation simulator.

Simulates the blast radius of a schema change (e.g. column rename) across
a unified cross-system lineage graph.

Algorithm:
  1. Start from the changed node (table/dataset/model).
  2. BFS traversal downstream through the unified graph.
  3. For each reached node, assign a severity:
       BREAKING  — direct consumer of the changed column
       WARNING   — indirect consumer (>1 hop away) or uses SELECT *
       OK        — not affected (different column path)
  4. Produce a blast radius report with counts per system and time-to-fix
     estimate based on node complexity.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx

# ---------------------------------------------------------------------------
# Enums and models
# ---------------------------------------------------------------------------


class Severity(str, Enum):
    BREAKING = "BREAKING"
    WARNING = "WARNING"
    OK = "OK"


@dataclass
class ImpactedNode:
    """A single downstream node affected by a change."""

    node_id: str
    label: str
    system: str
    node_type: str
    severity: Severity
    hop_distance: int
    path_from_source: List[str]
    reason: str
    # Estimated fix time in hours
    fix_hours: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "label": self.label,
            "system": self.system,
            "node_type": self.node_type,
            "severity": self.severity.value,
            "hop_distance": self.hop_distance,
            "path_from_source": self.path_from_source,
            "reason": self.reason,
            "fix_hours": self.fix_hours,
        }


@dataclass
class BlastRadiusReport:
    """Complete impact analysis report for a single schema change."""

    changed_node: str
    changed_column: Optional[str]
    change_type: str  # rename | drop | type_change | add
    total_impacted: int
    breaking_count: int
    warning_count: int
    ok_count: int
    impacted_nodes: List[ImpactedNode]
    # Per-system counts
    by_system: Dict[str, Dict[str, int]] = field(default_factory=dict)
    # Total estimated fix time
    total_fix_hours: float = 0.0

    def summary(self) -> str:
        lines = [
            f"=== Blast Radius Report ===",
            f"Changed node  : {self.changed_node}",
            f"Changed column: {self.changed_column or 'N/A'}",
            f"Change type   : {self.change_type}",
            f"",
            f"Total impacted: {self.total_impacted}",
            f"  BREAKING    : {self.breaking_count}",
            f"  WARNING     : {self.warning_count}",
            f"  OK          : {self.ok_count}",
            f"",
            f"Estimated fix : {self.total_fix_hours:.1f} hrs",
            f"",
            "By system:",
        ]
        for sys, counts in sorted(self.by_system.items()):
            lines.append(
                f"  {sys:12s}  BREAKING={counts.get('BREAKING', 0)}  WARNING={counts.get('WARNING', 0)}"
            )
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "changed_node": self.changed_node,
            "changed_column": self.changed_column,
            "change_type": self.change_type,
            "total_impacted": self.total_impacted,
            "breaking_count": self.breaking_count,
            "warning_count": self.warning_count,
            "ok_count": self.ok_count,
            "by_system": self.by_system,
            "total_fix_hours": self.total_fix_hours,
            "impacted_nodes": [n.to_dict() for n in self.impacted_nodes],
        }


# ---------------------------------------------------------------------------
# Fix time estimator
# ---------------------------------------------------------------------------

_BASE_FIX_HOURS: Dict[str, float] = {
    "sql": 1.0,
    "spark": 2.5,
    "dbt": 0.5,
    "airflow": 1.5,
    "unknown": 1.0,
}

_SEVERITY_MULTIPLIER: Dict[Severity, float] = {
    Severity.BREAKING: 1.0,
    Severity.WARNING: 0.4,
    Severity.OK: 0.0,
}


def _estimate_fix_hours(system: str, severity: Severity, hop_distance: int) -> float:
    """Estimate fix time based on system type, severity, and distance."""
    base = _BASE_FIX_HOURS.get(system, 1.0)
    mult = _SEVERITY_MULTIPLIER[severity]
    # Further downstream → slightly less work (change may already be caught earlier)
    distance_factor = max(0.5, 1.0 - (hop_distance - 1) * 0.1)
    return round(base * mult * distance_factor, 2)


# ---------------------------------------------------------------------------
# Propagation simulator
# ---------------------------------------------------------------------------


class PropagationSimulator:
    """
    Simulate change impact propagation through a cross-system lineage graph.

    Args:
        graph: A networkx DiGraph (from CrossSystemMerger.unified_graph).
    """

    def __init__(self, graph: nx.DiGraph) -> None:
        self._g = graph

    def simulate(
        self,
        node_id: str,
        column_name: Optional[str] = None,
        change_type: str = "rename",
    ) -> BlastRadiusReport:
        """
        Run BFS from `node_id` and classify downstream nodes.

        Args:
            node_id:     The node whose schema is changing.
            column_name: Specific column that changed (None = whole table).
            change_type: "rename" | "drop" | "type_change" | "add"

        Returns:
            BlastRadiusReport with full blast radius details.
        """
        if not self._g.has_node(node_id):
            return BlastRadiusReport(
                changed_node=node_id,
                changed_column=column_name,
                change_type=change_type,
                total_impacted=0,
                breaking_count=0,
                warning_count=0,
                ok_count=0,
                impacted_nodes=[],
            )

        impacted: List[ImpactedNode] = []
        visited: Set[str] = {node_id}

        # BFS queue: (current_node_id, hop_distance, path_so_far)
        queue: deque[Tuple[str, int, List[str]]] = deque()
        queue.append((node_id, 0, [node_id]))

        while queue:
            current_id, hop, path = queue.popleft()

            for successor in self._g.successors(current_id):
                if successor in visited:
                    continue
                visited.add(successor)

                edge_data = self._g.edges[current_id, successor]
                node_data = dict(self._g.nodes[successor])
                new_path = path + [successor]

                severity, reason = self._classify_severity(
                    edge_data=edge_data,
                    node_data=node_data,
                    column_name=column_name,
                    change_type=change_type,
                    hop=hop + 1,
                )

                system = node_data.get("system", "unknown")
                fix_hours = _estimate_fix_hours(system, severity, hop + 1)

                impacted.append(
                    ImpactedNode(
                        node_id=successor,
                        label=node_data.get("label", successor),
                        system=system,
                        node_type=node_data.get("node_type", "Unknown"),
                        severity=severity,
                        hop_distance=hop + 1,
                        path_from_source=new_path,
                        reason=reason,
                        fix_hours=fix_hours,
                    )
                )

                # Continue propagation for BREAKING and WARNING
                if severity in (Severity.BREAKING, Severity.WARNING):
                    queue.append((successor, hop + 1, new_path))

        # Sort by hop distance, then severity
        sev_order = {Severity.BREAKING: 0, Severity.WARNING: 1, Severity.OK: 2}
        impacted.sort(key=lambda n: (n.hop_distance, sev_order[n.severity]))

        # Aggregate counts
        breaking = sum(1 for n in impacted if n.severity == Severity.BREAKING)
        warning = sum(1 for n in impacted if n.severity == Severity.WARNING)
        ok = sum(1 for n in impacted if n.severity == Severity.OK)
        total_fix = sum(n.fix_hours for n in impacted)

        # Per-system breakdown
        by_system: Dict[str, Dict[str, int]] = {}
        for n in impacted:
            sys_dict = by_system.setdefault(n.system, {})
            sys_dict[n.severity.value] = sys_dict.get(n.severity.value, 0) + 1

        return BlastRadiusReport(
            changed_node=node_id,
            changed_column=column_name,
            change_type=change_type,
            total_impacted=len(impacted),
            breaking_count=breaking,
            warning_count=warning,
            ok_count=ok,
            impacted_nodes=impacted,
            by_system=by_system,
            total_fix_hours=round(total_fix, 2),
        )

    def _classify_severity(
        self,
        edge_data: Dict[str, Any],
        node_data: Dict[str, Any],
        column_name: Optional[str],
        change_type: str,
        hop: int,
    ) -> Tuple[Severity, str]:
        """
        Classify the severity of impact for a downstream node.

        Rules:
        - If no column_name specified → all consumers are BREAKING.
        - If column used directly in column_mappings → BREAKING.
        - If column_mappings contain wildcard (*) → WARNING.
        - If hop > 2 and not directly referencing column → WARNING.
        - Otherwise → OK.
        """
        if column_name is None:
            return (
                Severity.BREAKING,
                f"Whole-table change ({change_type}) propagates downstream",
            )

        column_mappings = edge_data.get("column_mappings", [])

        # Check if changed column appears in column mappings
        for mapping in column_mappings:
            src_expr = str(mapping.get("source_expression", ""))
            tgt_col = str(mapping.get("target_col", ""))
            if column_name.lower() in src_expr.lower():
                if change_type == "drop":
                    return (
                        Severity.BREAKING,
                        f"Column '{column_name}' is dropped; consumed in '{tgt_col}'",
                    )
                elif change_type == "rename":
                    return (
                        Severity.BREAKING,
                        f"Column '{column_name}' renamed; mapping '{src_expr}' → '{tgt_col}' will break",
                    )
                elif change_type == "type_change":
                    return (
                        Severity.WARNING,
                        f"Column '{column_name}' type changed; expression '{src_expr}' may be incompatible",
                    )
                else:
                    return (
                        Severity.WARNING,
                        f"Column '{column_name}' modified; review mapping '{src_expr}'",
                    )

        # Check for wildcard (* or SELECT *)
        for mapping in column_mappings:
            if str(mapping.get("source_expression", "")).strip() == "*":
                return (
                    Severity.WARNING,
                    f"Wildcard SELECT * — may implicitly include '{column_name}'",
                )
            if str(mapping.get("target_col", "")).strip() == "*":
                return (
                    Severity.WARNING,
                    f"Wildcard SELECT * — column '{column_name}' may be consumed",
                )

        # No direct column reference, but in propagation path
        if hop <= 2:
            return (
                Severity.WARNING,
                f"Indirect consumer at hop {hop}; review for implicit dependency on '{column_name}'",
            )

        return (
            Severity.OK,
            f"No direct dependency on '{column_name}' detected at hop {hop}",
        )

    def multi_column_simulate(
        self,
        node_id: str,
        column_changes: List[Dict[str, str]],
    ) -> List[BlastRadiusReport]:
        """
        Simulate multiple column changes at once.

        Args:
            node_id:         Node whose schema is changing.
            column_changes:  List of {"column": "col_name", "change_type": "rename|drop|..."}

        Returns:
            One BlastRadiusReport per column change.
        """
        reports = []
        for change in column_changes:
            report = self.simulate(
                node_id=node_id,
                column_name=change.get("column"),
                change_type=change.get("change_type", "rename"),
            )
            reports.append(report)
        return reports

    def top_risk_nodes(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """
        Rank nodes by their centrality (most downstream dependents).

        Returns the top N highest-risk nodes that, if changed, would impact the most nodes.
        """
        if len(self._g) == 0:
            return []

        try:
            centrality = nx.out_degree_centrality(self._g)
        except Exception:
            centrality = {n: self._g.out_degree(n) for n in self._g.nodes}

        ranked = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:top_n]
        result = []
        for nid, score in ranked:
            node_data = dict(self._g.nodes[nid])
            result.append(
                {
                    "node_id": nid,
                    "label": node_data.get("label", nid),
                    "system": node_data.get("system", "unknown"),
                    "centrality_score": round(score, 4),
                    "out_degree": self._g.out_degree(nid),
                }
            )
        return result
