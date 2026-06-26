"""CLI del pipeline de segmentación.

Cada etapa es invocable de forma aislada para reproducibilidad y depuración, y
``--build-all`` ejecuta el pipeline completo end-to-end:

    python -m olist_seg.cli --build-all
    python -m olist_seg.cli --build-features
    python -m olist_seg.cli --build-train
    python -m olist_seg.cli --build-score

Variables de entorno relevantes: MODEL_VERSION, OUTPUT_FORMAT (delta|parquet).
"""
from __future__ import annotations

import argparse
import logging
import sys

import mlflow
from pyspark.ml import PipelineModel

from olist_seg.config import Settings
from olist_seg.features import build_customer_features
from olist_seg.io import read_raw_customers, write_table
from olist_seg.score import score
from olist_seg.spark_session import build_spark
from olist_seg.train import train

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s"
)
log = logging.getLogger("olist_seg.cli")


def cmd_features(settings: Settings) -> None:
    spark = build_spark(settings.spark, delta=settings.output_format == "delta")
    raw = read_raw_customers(spark, settings.paths.raw_customers)
    feats = build_customer_features(raw)
    write_table(feats, settings.paths.silver_features, fmt=settings.output_format)
    log.info("Features escritas en %s (%d filas)", settings.paths.silver_features, feats.count())


def cmd_train(settings: Settings) -> None:
    spark = build_spark(settings.spark, delta=settings.output_format == "delta")
    feats = spark.read.format(settings.output_format).load(settings.paths.silver_features)
    model = train(feats, settings.model, settings.model_version)
    model.write().overwrite().save(settings.paths.model_dir)
    log.info("Modelo guardado en %s", settings.paths.model_dir)


def cmd_score(settings: Settings) -> None:
    spark = build_spark(settings.spark, delta=settings.output_format == "delta")
    feats = spark.read.format(settings.output_format).load(settings.paths.silver_features)
    model = PipelineModel.load(settings.paths.model_dir)
    result = score(model, feats, settings.model_version)
    write_table(
        result,
        settings.paths.gold_segments,
        fmt=settings.output_format,
        partition_by=["primary_region"],
    )
    log.info("Segmentos escritos en %s (%d filas)", settings.paths.gold_segments, result.count())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Olist customer segmentation pipeline")
    parser.add_argument("--config", default="conf/feature_config.yml")
    parser.add_argument("--build-features", action="store_true")
    parser.add_argument("--build-train", action="store_true")
    parser.add_argument("--build-score", action="store_true")
    parser.add_argument("--build-all", action="store_true")
    args = parser.parse_args(argv)

    settings = Settings.load(args.config)
    mlflow.set_experiment("/olist-customer-segmentation")

    ran = False
    if args.build_all or args.build_features:
        cmd_features(settings); ran = True
    if args.build_all or args.build_train:
        cmd_train(settings); ran = True
    if args.build_all or args.build_score:
        cmd_score(settings); ran = True

    if not ran:
        parser.error("Indica al menos una etapa: --build-features|--build-train|--build-score|--build-all")
    return 0


if __name__ == "__main__":
    sys.exit(main())
