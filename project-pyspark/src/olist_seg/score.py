"""Scoring batch y traducción de clusters a etiquetas de negocio.

Los IDs de cluster de K-Means son arbitrarios; para que el dashboard de BI sea
estable entre reentrenamientos, derivamos una etiqueta interpretable a partir del
perfil de cada cluster (frecuencia media y dispersión geográfica)."""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def label_segments(scored: DataFrame) -> DataFrame:
    """Asigna una etiqueta de negocio por cluster según su perfil promedio.

    Reglas (interpretables y auditables):
      * 'Recurrente'        -> frecuencia media de órdenes alta (> 1.3)
      * 'Itinerante'        -> compra desde varios estados (movilidad alta)
      * 'Metropolitano'     -> alta concentración de mercado (state_market_share alto)
      * 'Estándar regional' -> resto
    """
    profile = scored.groupBy("segment_id").agg(
        F.avg("num_orders").alias("avg_orders"),
        F.avg("num_distinct_states").alias("avg_states"),
        F.avg("state_market_share").alias("avg_share"),
    )

    labeled = profile.withColumn(
        "segment_label",
        F.when(F.col("avg_orders") > 1.3, F.lit("Recurrente"))
        .when(F.col("avg_states") > 1.05, F.lit("Itinerante"))
        .when(F.col("avg_share") > 0.30, F.lit("Metropolitano"))
        .otherwise(F.lit("Estándar regional")),
    ).select("segment_id", "segment_label")

    return scored.join(F.broadcast(labeled), on="segment_id", how="left")


def score(model: PipelineModel, features: DataFrame, model_version: str) -> DataFrame:
    """Aplica el modelo y produce la tabla gold lista para BI."""
    scored = model.transform(features)
    scored = label_segments(scored)
    return scored.select(
        "customer_unique_id",
        "primary_customer_id",
        "num_orders",
        "is_repeat_customer",
        "num_distinct_states",
        "num_distinct_cities",
        "zip_macro_region",
        "primary_state",
        "primary_region",
        "state_market_share",
        "segment_id",
        "segment_label",
        F.lit(model_version).alias("model_version"),
        F.lit(datetime.now(timezone.utc).isoformat()).alias("scored_at"),
    )
