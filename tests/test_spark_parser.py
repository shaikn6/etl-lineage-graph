"""Tests for parsers/spark_lineage_parser.py — PySpark AST lineage extraction."""

from __future__ import annotations

import textwrap

import pytest

from parsers.spark_lineage_parser import (
    SparkDataset,
    SparkLineageNode,
    SparkTransformation,
    parse_spark_code,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse(code: str, name: str = "test_pipeline") -> SparkLineageNode:
    return parse_spark_code(textwrap.dedent(code), pipeline_name=name)


# ---------------------------------------------------------------------------
# Source detection
# ---------------------------------------------------------------------------

class TestSparkSourceDetection:
    def test_read_parquet(self):
        node = _parse("""
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://bucket/data/")
        """)
        assert len(node.sources) == 1
        assert node.sources[0].path == "s3://bucket/data/"
        assert node.sources[0].format == "parquet"

    def test_read_csv(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.csv("s3://bucket/csv_data/")
        """)
        assert any(s.format == "csv" for s in node.sources)

    def test_read_json(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.json("s3://bucket/events/")
        """)
        assert any(s.format == "json" for s in node.sources)

    def test_multiple_reads(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            orders = spark.read.parquet("s3://lake/orders/")
            events = spark.read.parquet("s3://lake/events/")
        """)
        paths = [s.path for s in node.sources]
        assert "s3://lake/orders/" in paths
        assert "s3://lake/events/" in paths

    def test_read_dataset_type_is_source(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/b/")
        """)
        assert node.sources[0].dataset_type == "source"

    def test_source_paths_property(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://x/y/")
        """)
        assert "s3://x/y/" in node.source_paths


# ---------------------------------------------------------------------------
# Sink detection
# ---------------------------------------------------------------------------

class TestSparkSinkDetection:
    def test_write_parquet(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            df.write.parquet("s3://b/output/")
        """)
        assert len(node.sinks) == 1
        assert node.sinks[0].path == "s3://b/output/"
        assert node.sinks[0].format == "parquet"

    def test_write_csv(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            df.write.csv("s3://b/csv/")
        """)
        assert any(s.format == "csv" for s in node.sinks)

    def test_multiple_sinks(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            df.write.parquet("s3://b/out1/")
            df.write.parquet("s3://b/out2/")
        """)
        assert len(node.sinks) == 2

    def test_sink_paths_property(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            df.write.parquet("s3://b/")
        """)
        assert "s3://b/" in node.sink_paths

    def test_dataset_type_is_sink(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            df.write.parquet("s3://b/")
        """)
        assert node.sinks[0].dataset_type == "sink"


# ---------------------------------------------------------------------------
# Transformation detection
# ---------------------------------------------------------------------------

class TestSparkTransformations:
    def test_filter_detected(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            clean = df.filter("status = 'ok'")
            clean.write.parquet("s3://b/")
        """)
        ops = [t.operation for t in node.transformations]
        assert "filter" in ops

    def test_select_detected(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            selected = df.select("id", "amount")
            selected.write.parquet("s3://b/")
        """)
        ops = [t.operation for t in node.transformations]
        assert "select" in ops

    def test_groupby_detected(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3://a/")
            grouped = df.groupBy("region")
            grouped.write.parquet("s3://b/")
        """)
        ops = [t.operation for t in node.transformations]
        assert "groupBy" in ops

    def test_join_detected(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df1 = spark.read.parquet("s3://a/")
            df2 = spark.read.parquet("s3://b/")
            joined = df1.join(df2, on="id", how="left")
            joined.write.parquet("s3://c/")
        """)
        ops = [t.operation for t in node.transformations]
        assert "join" in ops

    def test_join_captures_both_inputs(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            df1 = spark.read.parquet("s3://a/")
            df2 = spark.read.parquet("s3://b/")
            joined = df1.join(df2, on="id")
            joined.write.parquet("s3://c/")
        """)
        join_t = next((t for t in node.transformations if t.operation == "join"), None)
        assert join_t is not None
        assert len(join_t.input_vars) == 2

    def test_pipeline_name_set(self):
        node = _parse("spark = None", name="my_pipeline")
        assert node.pipeline_name == "my_pipeline"

    def test_empty_code_returns_no_sources(self):
        node = _parse("")
        assert node.sources == []
        assert node.sinks == []


# ---------------------------------------------------------------------------
# Multi-step pipeline
# ---------------------------------------------------------------------------

class TestMultiStepPipeline:
    def test_chain_of_transforms(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            raw = spark.read.parquet("s3://raw/")
            clean = raw.filter("status = 'ok'")
            selected = clean.select("id", "amount")
            selected.write.parquet("s3://processed/")
        """)
        ops = [t.operation for t in node.transformations]
        assert "filter" in ops
        assert "select" in ops

    def test_intermediate_vars_tracked(self):
        node = _parse("""
            spark = SparkSession.builder.getOrCreate()
            raw = spark.read.parquet("s3://raw/")
            clean = raw.filter("active = true")
            clean.write.parquet("s3://out/")
        """)
        # raw is source, clean is intermediate
        assert "s3://raw/" in node.source_paths
        assert "s3://out/" in node.sink_paths
