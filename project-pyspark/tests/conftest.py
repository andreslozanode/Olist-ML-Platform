"""Fixtures compartidas para los tests. SparkSession local de un solo proceso."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    s = (
        SparkSession.builder.master("local[1]")
        .appName("olist-seg-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture()
def raw_df(spark: SparkSession):
    """Dataset mínimo: 1 cliente recurrente (2 órdenes) + 2 de una sola orden."""
    rows = [
        ("ord1", "personA", "01001", "sao paulo", "SP"),
        ("ord2", "personA", "01002", "sao paulo", "SP"),  # recurrente
        ("ord3", "personB", "20000", "rio de janeiro", "RJ"),
        ("ord4", "personC", "90000", "porto alegre", "RS"),
    ]
    cols = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ]
    return spark.createDataFrame(rows, cols)
