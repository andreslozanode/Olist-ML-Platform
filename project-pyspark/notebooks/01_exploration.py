# Databricks notebook source
# MAGIC %md
# MAGIC # Exploración — Segmentación de clientes Olist
# MAGIC Notebook de exploración. La lógica productiva vive en `src/olist_seg` y se
# MAGIC despliega como wheel vía Databricks Asset Bundle.

# COMMAND ----------
from olist_seg.config import Settings
from olist_seg.spark_session import build_spark
from olist_seg.io import read_raw_customers
from olist_seg.features import build_customer_features

settings = Settings.load("conf/feature_config.yml")
spark = build_spark(settings.spark, delta=True)

# COMMAND ----------
raw = read_raw_customers(spark, settings.paths.raw_customers)
feats = build_customer_features(raw)
display(feats.groupBy("primary_region", "is_repeat_customer").count())

# COMMAND ----------
# MAGIC %md Distribución de frecuencia (proxy RFM sin tabla de órdenes)
display(feats.groupBy("num_orders").count().orderBy("num_orders"))
