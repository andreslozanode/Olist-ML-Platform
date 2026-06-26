"""Ingeniería de features a grano ``customer_unique_id`` (persona real).

En Olist ``customer_id`` es por orden y ``customer_unique_id`` es la persona. Por eso
agregamos a nivel persona para construir señales tipo RFM-lite que NO requieren la
tabla de órdenes:
  * num_orders          -> proxy de Frecuencia (conteo de customer_id por persona)
  * is_repeat_customer  -> flag binario de recurrencia
  * num_distinct_states / num_distinct_cities -> movilidad geográfica
  * primary_state / primary_region            -> ubicación dominante
  * zip_macro_region    -> primer dígito del CEP (macro-región logística de Brasil)
  * state_market_share  -> peso del estado en la base (densidad de mercado)
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Mapa estado -> región (5 macro-regiones IBGE). Pequeño: ideal para broadcast.
STATE_REGION = {
    # Norte
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte",
    "RO": "Norte", "RR": "Norte", "TO": "Norte",
    # Nordeste
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste",
    "SE": "Nordeste",
    # Centro-Oeste
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste",
    "MS": "Centro-Oeste",
    # Sudeste
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    # Sul
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}
REGION_INDEX = {r: i for i, r in enumerate(sorted(set(STATE_REGION.values())))}


def _str_map(d: dict) -> F.Column:
    """Construye un map literal de Spark a partir de un dict de Python."""
    return F.create_map([F.lit(x) for kv in d.items() for x in kv])


def build_customer_features(raw: DataFrame) -> DataFrame:
    """Transforma el dataset crudo (1 fila = 1 orden) en features a grano persona."""
    region_map = _str_map(STATE_REGION)
    region_idx_map = _str_map(REGION_INDEX)

    # 1) Enriquecer cada orden con región y macro-región CEP (primer dígito).
    enriched = raw.withColumn("region", region_map[F.col("customer_state")]).withColumn(
        "zip_macro_region",
        F.coalesce(
            F.substring(F.col("customer_zip_code_prefix"), 1, 1).cast("int"), F.lit(-1)
        ),
    )

    # 2) Market share por estado (densidad de mercado) sobre el total de órdenes.
    total_orders = enriched.count()
    state_share = (
        enriched.groupBy("customer_state")
        .agg(F.count("*").alias("state_orders"))
        .withColumn("state_market_share", F.col("state_orders") / F.lit(total_orders))
        .select(
            F.col("customer_state").alias("ss_state"), "state_market_share"
        )
    )

    # 3) Estado/región dominante por persona (mayor frecuencia, desempate por estado).
    w = Window.partitionBy("customer_unique_id").orderBy(
        F.desc("cnt"), F.asc("customer_state")
    )
    primary = (
        enriched.groupBy("customer_unique_id", "customer_state", "region")
        .agg(F.count("*").alias("cnt"))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "customer_unique_id",
            F.col("customer_state").alias("primary_state"),
            F.col("region").alias("primary_region"),
        )
    )

    # 4) Agregados por persona.
    agg = enriched.groupBy("customer_unique_id").agg(
        F.count("*").alias("num_orders"),
        F.first("customer_id").alias("primary_customer_id"),
        F.countDistinct("customer_state").alias("num_distinct_states"),
        F.countDistinct("customer_city").alias("num_distinct_cities"),
        F.max("zip_macro_region").alias("zip_macro_region"),
    )

    features = (
        agg.join(primary, on="customer_unique_id", how="left")
        .join(F.broadcast(state_share), F.col("primary_state") == F.col("ss_state"), "left")
        .withColumn("is_repeat_customer", (F.col("num_orders") > 1).cast("int"))
        .withColumn(
            "region_idx",
            F.coalesce(region_idx_map[F.col("primary_region")], F.lit(-1)),
        )
        .withColumn(
            "state_market_share", F.coalesce(F.col("state_market_share"), F.lit(0.0))
        )
    )

    return features.select(
        "customer_unique_id",
        "primary_customer_id",
        "num_orders",
        "is_repeat_customer",
        "num_distinct_states",
        "num_distinct_cities",
        "zip_macro_region",
        "primary_state",
        "primary_region",
        "region_idx",
        "state_market_share",
    )
