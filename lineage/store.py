"""PostgreSQL persistence layer for lineage nodes and edges."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, Integer, String, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import Session, declarative_base, sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()


class LineageNodeRecord(Base):
    __tablename__ = "lineage_nodes"

    table_name = Column(String(255), primary_key=True)
    schema_name = Column(String(255), nullable=True)
    row_count_estimate = Column(Integer, nullable=True)
    last_updated = Column(String(50), nullable=True)
    is_source = Column(String(5), nullable=True, default="true")
    pipeline_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text("NOW()"))


class LineageEdgeRecord(Base):
    __tablename__ = "lineage_edges"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_table = Column(String(255), nullable=False)
    target_table = Column(String(255), nullable=False)
    column_mappings = Column(JSONB, nullable=True)
    transformation_type = Column(String(50), nullable=True)
    pipeline_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=text("NOW()"))


class LineageStore:
    """Persist and retrieve lineage graph from PostgreSQL."""

    def __init__(self, database_url: str) -> None:
        self._engine = create_engine(database_url, echo=False, future=False)
        self._SessionLocal = sessionmaker(bind=self._engine)
        Base.metadata.create_all(self._engine)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def upsert_node(
        self,
        table_name: str,
        schema_name: Optional[str] = None,
        row_count_estimate: Optional[int] = None,
        last_updated: Optional[str] = None,
        is_source: bool = True,
        pipeline_name: Optional[str] = None,
    ) -> None:
        stmt = insert(LineageNodeRecord).values(
            table_name=table_name,
            schema_name=schema_name,
            row_count_estimate=row_count_estimate,
            last_updated=last_updated,
            is_source=str(is_source).lower(),
            pipeline_name=pipeline_name,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["table_name"],
            set_={
                "schema_name": schema_name,
                "row_count_estimate": row_count_estimate,
                "last_updated": last_updated,
                "is_source": str(is_source).lower(),
                "pipeline_name": pipeline_name,
            },
        )
        with Session(self._engine) as session:
            session.execute(stmt)
            session.commit()

    def upsert_edge(
        self,
        source_table: str,
        target_table: str,
        column_mappings: Optional[List[Dict[str, Any]]] = None,
        transformation_type: Optional[str] = None,
        pipeline_name: Optional[str] = None,
    ) -> None:
        """Upsert by deleting the existing edge (source, target) and re-inserting."""
        with Session(self._engine) as session:
            session.execute(
                text(
                    "DELETE FROM lineage_edges WHERE source_table = :src AND target_table = :tgt"
                ),
                {"src": source_table, "tgt": target_table},
            )
            edge = LineageEdgeRecord(
                source_table=source_table,
                target_table=target_table,
                column_mappings=column_mappings or [],
                transformation_type=transformation_type,
                pipeline_name=pipeline_name,
            )
            session.add(edge)
            session.commit()

    def persist_graph(self, graph_dict: Dict[str, Any]) -> None:
        """Persist the full graph returned by LineageGraph.as_dict()."""
        for node in graph_dict.get("nodes", []):
            self.upsert_node(
                table_name=node["table"],
                schema_name=node.get("schema"),
                row_count_estimate=node.get("row_count_estimate"),
                last_updated=node.get("last_updated"),
                is_source=node.get("is_source", True),
            )
        for edge in graph_dict.get("edges", []):
            self.upsert_edge(
                source_table=edge["source"],
                target_table=edge["target"],
                column_mappings=edge.get("column_mappings", []),
                transformation_type=edge.get("transformation_type"),
                pipeline_name=edge.get("pipeline_name"),
            )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load_all_nodes(self) -> List[Dict[str, Any]]:
        with Session(self._engine) as session:
            rows = session.query(LineageNodeRecord).all()
            return [
                {
                    "table": r.table_name,
                    "schema": r.schema_name,
                    "row_count_estimate": r.row_count_estimate,
                    "last_updated": r.last_updated,
                    "is_source": r.is_source == "true",
                    "pipeline_name": r.pipeline_name,
                }
                for r in rows
            ]

    def load_all_edges(self) -> List[Dict[str, Any]]:
        with Session(self._engine) as session:
            rows = session.query(LineageEdgeRecord).all()
            return [
                {
                    "source": r.source_table,
                    "target": r.target_table,
                    "column_mappings": r.column_mappings or [],
                    "transformation_type": r.transformation_type,
                    "pipeline_name": r.pipeline_name,
                }
                for r in rows
            ]

    def health_check(self) -> bool:
        try:
            with Session(self._engine) as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("DB health check failed: %s", exc)
            return False
