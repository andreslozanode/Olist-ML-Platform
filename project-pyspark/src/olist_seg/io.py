"""Lectura/escritura de datos. Schema explícito (no inferencia) para reproducibilidad
y para evitar el clásico bug de tipos donde el zip se infiere como int y pierde ceros."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

RAW_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("customer_unique_id", StringType(), nullable=False),
        StructField("customer_zip_code_prefix", StringType(), nullable=True),
        StructField("customer_city", StringType(), nullable=True),
        StructField("customer_state", StringType(), nullable=True),
    ]
)


def read_raw_customers(spark: SparkSession, path: str) -> DataFrame:
    """Lee el CSV crudo con schema fijo. El zip se mantiene como string para
    preservar ceros a la izquierda (CEP brasileño)."""
    return (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "DROPMALFORMED")
        .schema(RAW_SCHEMA)
        .csv(path)
    )


def write_table(
    df: DataFrame,
    path: str,
    *,
    fmt: str = "delta",
    partition_by: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    """Escritura idempotente. Particionar por una columna de baja cardinalidad
    (p.ej. primary_region) mejora el pruning en lecturas de BI."""
    writer = df.write.mode(mode).format(fmt)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if fmt == "parquet":
        writer = writer.option("compression", "snappy")
    writer.save(path)
