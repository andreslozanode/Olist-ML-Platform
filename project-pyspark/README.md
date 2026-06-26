# Proyecto A — Segmentación de clientes (PySpark + MLflow)

Pipeline de ML **no supervisado** (K-Means) que segmenta a los clientes de Olist a
grano de **persona** (`customer_unique_id`), listo para ejecutarse en local o en
**Databricks** y producir una tabla *gold* consumible por BI.

## Problema de ML
Segmentación de clientes con señales tipo **RFM-lite** que no requieren la tabla de
órdenes (solo `customers`):

| Feature | Significado |
|---|---|
| `num_orders` | Frecuencia (proxy R/F): nº de `customer_id` por persona |
| `is_repeat_customer` | Recurrencia (1 si compró >1 vez) |
| `num_distinct_states` / `num_distinct_cities` | Movilidad geográfica |
| `zip_macro_region` | Macro-región logística (1er dígito del CEP) |
| `state_market_share` | Densidad de mercado del estado |
| `region_idx` | Región IBGE codificada |

## Arquitectura (medallion)
`raw CSV` → **bronze** → **silver** (features, grano persona) → **gold** (segmentos) → BI

## Optimizaciones de Spark
- **AQE** (coalesce de particiones + skew join handling).
- `shuffle.partitions` afinadas (64, no 200) al volumen real.
- **Broadcast join** del mapa estado→región y del market share (<27 filas).
- `cache()` del DataFrame de features durante el barrido de `k`.
- Escritura **particionada por `primary_region`** + Delta `optimizeWrite`/`autoCompact`.
- Serialización **Kryo** y compresión **snappy**.

## Uso
```bash
pip install -e ".[dev]"

# Pipeline completo
python -m olist_seg.cli --build-all

# Etapas individuales (reproducibilidad)
python -m olist_seg.cli --build-features
python -m olist_seg.cli --build-train
python -m olist_seg.cli --build-score

# Tests
pytest
```

## Despliegue (Databricks Asset Bundle)
```bash
python -m build --wheel          # genera dist/*.whl
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run olist_segmentation_pipeline -t dev
```
El job corre tres tareas encadenadas (`build_features` → `build_train` → `build_score`)
con un cluster autoescalable y agenda diaria. El modelo y métricas quedan en **MLflow**.

## Salida para BI
`data/gold/customer_segments` (Delta/Parquet, particionado por `primary_region`):
`customer_unique_id, num_orders, is_repeat_customer, primary_state, primary_region,
state_market_share, segment_id, segment_label, model_version, scored_at`.
