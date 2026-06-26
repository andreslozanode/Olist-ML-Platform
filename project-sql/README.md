# Proyecto B — Segmentación de clientes (SQL · Snowflake + dbt + Snowpark ML)

Misma segmentación que el Proyecto A, pero **100% sobre Snowflake**: transformaciones
con **dbt**, modelo K-Means con **Snowpark ML** ejecutándose dentro del warehouse, y
migraciones de infraestructura con **schemachange**. El resultado es una *mart* ancha
lista para BI.

## Componentes
```
snowflake/migrations/   Infra (schemachange): DB, schemas, warehouses, RBAC, cluster keys
snowflake/ml/           Modelo K-Means (Snowpark ML) + task de reentrenamiento
models/staging/         Limpieza/tipado (grano orden)
models/intermediate/    Features grano persona + baseline rule-based (ephemeral)
models/marts/           dim_customers, fct_customer_segments, mart_bi_customer_segments
seeds/                  br_state_region.csv (mapa estado→región)
macros/                 grants a BI_READER, naming de schemas
```

## Flujo
1. **schemachange** crea DB/schemas/warehouses/roles y carga el CSV a `RAW`.
2. **dbt** construye staging → intermediate → marts.
3. **Snowpark ML** (`SP_TRAIN_KMEANS_SEGMENTS`) entrena K-Means sobre los features y
   materializa `ANALYTICS.ML.CUSTOMER_SEGMENTS_ML`.
4. `fct_customer_segments` une el resultado ML; si la tabla ML no existe, usa el
   **baseline SQL** (`int_segments_rule_based`) — el pipeline nunca se rompe.

## Optimizaciones de Snowflake
- **Cluster keys** `(primary_region, segment_id)` en la mart de BI → *pruning* de
  micro-particiones en filtros típicos de dashboards.
- **Automatic clustering** (`RESUME RECLUSTER`) para mantener el orden con el tiempo.
- **Warehouses separados** por workload (`LOAD_WH` / `TRANSFORM_WH` / `ML_WH`) con
  `AUTO_SUSPEND=60` → aislamiento de costos.
- Modelos intermedios **ephemeral** (no materializan, se inlinean).
- `SYSTEM$CLUSTERING_INFORMATION` para diagnosticar calidad de clustering.

## Uso
```bash
pip install -r requirements.txt
dbt deps

# 1) Infra + carga
schemachange -f snowflake/migrations --root-folder snowflake/migrations

# 2) Transformaciones
dbt seed && dbt run && dbt test

# 3) Modelo ML en Snowflake
snow sql -f snowflake/ml/sp_train_kmeans_segments.sql
snow sql -f snowflake/ml/sp_apply_segments.sql
snow sql -q "CALL ANALYTICS.ML.SP_APPLY_SEGMENTS(5);"

# 4) Reconstruir marts con la asignación ML
dbt run --select marts
```

## Salida para BI
`ANALYTICS.MARTS.MART_BI_CUSTOMER_SEGMENTS` — una fila por persona, con dimensiones,
métricas y `segment_id`/`segment_label`. `BI_READER` tiene `SELECT` sobre el schema.
