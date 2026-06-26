"""Entrenamiento del modelo de segmentación.

Pipeline de Spark ML: VectorAssembler -> StandardScaler -> KMeans.
Selecciona k por coeficiente de silueta dentro de un rango configurable y registra
métricas/artefactos en MLflow (compatible con MLflow local y con Databricks).
"""
from __future__ import annotations

import logging

import mlflow
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame

from olist_seg.config import ModelParams

log = logging.getLogger("olist_seg.train")


def _build_pipeline(features: list[str], k: int, max_iter: int, seed: int) -> Pipeline:
    assembler = VectorAssembler(
        inputCols=features, outputCol="features_raw", handleInvalid="keep"
    )
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features", withMean=True, withStd=True
    )
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="segment_id",
        k=k,
        maxIter=max_iter,
        seed=seed,
    )
    return Pipeline(stages=[assembler, scaler, kmeans])


def select_k(df: DataFrame, params: ModelParams) -> tuple[int, float]:
    """Barrido de k maximizando la silueta (squaredEuclidean)."""
    evaluator = ClusteringEvaluator(
        featuresCol="features", predictionCol="segment_id", metricName="silhouette"
    )
    best_k, best_score = params.k_search_min, -1.0
    for k in range(params.k_search_min, params.k_search_max + 1):
        model = _build_pipeline(params.features, k, params.max_iter, params.seed).fit(df)
        score = evaluator.evaluate(model.transform(df))
        log.info("k=%d silhouette=%.4f", k, score)
        mlflow.log_metric(f"silhouette_k{k}", score)
        if score > best_score:
            best_k, best_score = k, score
    return best_k, best_score


def train(df: DataFrame, params: ModelParams, model_version: str) -> PipelineModel:
    """Entrena el modelo final y lo registra en MLflow."""
    df = df.cache()
    with mlflow.start_run(run_name=f"kmeans_{model_version}"):
        mlflow.log_params(
            {
                "k_fixed": params.k,
                "max_iter": params.max_iter,
                "seed": params.seed,
                "n_features": len(params.features),
                "model_version": model_version,
            }
        )
        best_k, best_score = select_k(df, params)
        mlflow.log_param("k_selected", best_k)
        mlflow.log_metric("silhouette_best", best_score)

        model = _build_pipeline(params.features, best_k, params.max_iter, params.seed).fit(df)

        kmeans_model = model.stages[-1]
        mlflow.log_metric("training_cost_wssse", kmeans_model.summary.trainingCost)
        log.info("Modelo final entrenado con k=%d (silhouette=%.4f)", best_k, best_score)
    df.unpersist()
    return model
