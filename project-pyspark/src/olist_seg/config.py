"""Configuración centralizada del pipeline de segmentación.

Se carga desde ``conf/feature_config.yml`` y/o variables de entorno, de modo que
el mismo código corra en local (Spark standalone) y en Databricks sin cambios.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Paths:
    """Rutas de entrada/salida. En Databricks se sobrescriben con rutas de Volumes/DBFS."""

    raw_customers: str = "data/raw/olist_customers_dataset.csv"
    bronze: str = "data/bronze/customers"
    silver_features: str = "data/silver/customer_features"
    gold_segments: str = "data/gold/customer_segments"
    model_dir: str = "artifacts/model"


@dataclass(frozen=True)
class ModelParams:
    """Hiperparámetros de K-Means y de la búsqueda de k."""

    k: int = 5
    k_search_min: int = 3
    k_search_max: int = 8
    max_iter: int = 50
    seed: int = 42
    features: list[str] = field(
        default_factory=lambda: [
            "num_orders",
            "is_repeat_customer",
            "num_distinct_states",
            "num_distinct_cities",
            "zip_macro_region",
            "state_market_share",
            "region_idx",
        ]
    )


@dataclass(frozen=True)
class SparkConf:
    """Flags de optimización aplicados a la SparkSession."""

    app_name: str = "olist-customer-segmentation"
    shuffle_partitions: int = 64
    aqe_enabled: bool = True
    broadcast_threshold_mb: int = 32


@dataclass(frozen=True)
class Settings:
    paths: Paths = field(default_factory=Paths)
    model: ModelParams = field(default_factory=ModelParams)
    spark: SparkConf = field(default_factory=SparkConf)
    model_version: str = "v1.0.0"
    output_format: str = "delta"  # "delta" en Databricks, "parquet" en local sin delta

    @staticmethod
    def load(path: str | Path | None = None) -> "Settings":
        """Carga settings desde YAML; las variables de entorno tienen prioridad."""
        data: dict[str, Any] = {}
        if path and Path(path).exists():
            data = yaml.safe_load(Path(path).read_text()) or {}

        paths = Paths(**data.get("paths", {}))
        model = ModelParams(**data.get("model", {}))
        spark = SparkConf(**data.get("spark", {}))

        return Settings(
            paths=paths,
            model=model,
            spark=spark,
            model_version=os.getenv("MODEL_VERSION", data.get("model_version", "v1.0.0")),
            output_format=os.getenv("OUTPUT_FORMAT", data.get("output_format", "delta")),
        )
