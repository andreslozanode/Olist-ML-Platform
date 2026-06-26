"""Construcción de la SparkSession con optimizaciones activadas por defecto.

Optimizaciones aplicadas:
  * Adaptive Query Execution (AQE): coalesce de particiones, skew join handling.
  * shuffle partitions afinadas al volumen (no los 200 por defecto).
  * autoBroadcastJoinThreshold ajustado para joins con tablas dimensionales pequeñas
    (mapa estado->región), evitando shuffles innecesarios.
  * Serialización Kryo y compresión snappy en el shuffle.
Estas opciones son idempotentes: en Databricks se respetan las del cluster y solo
añadimos las específicas del job.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from olist_seg.config import SparkConf


def build_spark(conf: SparkConf, *, delta: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(conf.app_name)
        .config("spark.sql.adaptive.enabled", str(conf.aqe_enabled).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", conf.shuffle_partitions)
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            conf.broadcast_threshold_mb * 1024 * 1024,
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if delta:
        # Solo cuando delta-spark está instalado (local) o en Databricks Runtime.
        builder = (
            builder.config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
