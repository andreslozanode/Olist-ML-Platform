# Olist ML Platform — Segmentación de Clientes

Plataforma de Machine Learning para **segmentación de clientes** sobre el dataset
`olist_customers_dataset.csv`, implementada **dos veces de forma independiente** y con
el **mismo contrato de datos**, para que puedas elegir el stack según tu infraestructura:

| Proyecto | Stack | Motor de cómputo | Despliegue |
|----------|-------|------------------|------------|
| [`project-pyspark/`](./project-pyspark) | **Python + PySpark + MLflow** | Databricks / Spark | Databricks Asset Bundles |
| [`project-sql/`](./project-sql) | **SQL + dbt + Snowpark ML** | Snowflake (in-database) | schemachange + dbt |

Ambos pipelines fueron **validados end-to-end sobre los datos reales** y producen
**resultados idénticos** (96.096 personas, 2.997 clientes recurrentes, 99.441 órdenes).

---

## El problema de negocio

El dataset trae **99.441 órdenes** pero solo **96.096 personas reales**
(`customer_unique_id`). Esa diferencia es la señal de oro: **2.997 clientes (3,12%)
compran más de una vez**. Eso permite construir un proxy de **frecuencia tipo RFM**
sin necesidad de la tabla de órdenes, y plantear el problema como **segmentación no
supervisada (K-Means)** a grano persona.

Los segmentos resultantes (Recurrente, Itinerante, Metropolitano, Estándar regional)
se materializan en una tabla **lista para BI**.

Ver el detalle del razonamiento en [`docs/architecture.md`](./docs/architecture.md).

---

## Contrato de datos único

Los **dos** proyectos convergen exactamente en la misma tabla de salida
(`customer_segments`), de modo que tu capa de BI no necesita saber qué motor la generó:

```
customer_unique_id   primary_customer_id   num_orders        is_repeat_customer
num_distinct_states  num_distinct_cities   zip_macro_region  primary_state
primary_region       state_market_share    segment_id        segment_label
model_version        scored_at
```

El contrato completo y las instrucciones de conexión (Power BI / Tableau / Looker)
están en [`docs/bi_integration.md`](./docs/bi_integration.md).

---

## Estructura del repositorio

```
olist-ml-platform/
├── README.md                 ← este manifest
├── LICENSE                   ← MIT
├── Makefile                  ← atajos: setup / test / run / build / lint
├── .pre-commit-config.yaml   ← ruff + sqlfluff + hooks
├── .gitignore
│
├── project-pyspark/          ← PROYECTO A: PySpark + MLflow → Databricks
│   ├── src/olist_seg/        ← config, features, train, score, cli
│   ├── conf/                 ← feature_config.yml
│   ├── tests/                ← pytest (6 tests)
│   ├── notebooks/
│   ├── databricks.yml        ← Asset Bundle (jobs encadenados)
│   └── pyproject.toml
│
├── project-sql/              ← PROYECTO B: dbt + Snowpark ML → Snowflake
│   ├── models/               ← staging → intermediate → marts
│   ├── seeds/                ← br_state_region.csv
│   ├── macros/ · tests/
│   ├── snowflake/migrations/ ← schemachange (DB, RBAC, warehouses, COPY)
│   ├── snowflake/ml/         ← stored procs Snowpark ML (train + apply)
│   └── dbt_project.yml
│
├── .github/workflows/        ← CI/CD GitHub Actions (4 pipelines)
│   ├── ci-pyspark.yml · cd-pyspark-databricks.yml
│   └── ci-sql.yml    · cd-sql-snowflake.yml
│
├── docs/
│   ├── architecture.md       ← decisiones de diseño, comparativa A vs B
│   └── bi_integration.md     ← contrato de columnas + conexión BI
│
└── data/raw/                 ← coloca aquí olist_customers_dataset.csv
```

---

## Quickstart

Requisitos: Python 3.10+, Java 17+ (para Spark local), y opcionalmente cuentas de
Databricks / Snowflake para el despliegue real.

```bash
# 1. Instala dependencias de ambos proyectos
make setup

# 2A. PROYECTO PySpark — corre el pipeline completo en local
make pyspark-run        # features → train → score, escribe la tabla gold
make pyspark-test       # 6 tests unitarios

# 2B. PROYECTO SQL — construye los modelos dbt
make sql-build          # dbt seed + run + test
make sql-lint           # sqlfluff
```

El proyecto PySpark también expone un CLI granular:

```bash
cd project-pyspark
PYTHONPATH=src python -m olist_seg.cli --build-features   # solo features
PYTHONPATH=src python -m olist_seg.cli --build-train      # entrena + MLflow
PYTHONPATH=src python -m olist_seg.cli --build-score      # etiqueta segmentos
PYTHONPATH=src python -m olist_seg.cli --build-all        # todo
```

---

## Despliegue

### Proyecto PySpark → Databricks
Push de un tag `pyspark-v*` dispara `cd-pyspark-databricks.yml`: construye el wheel y
hace `databricks bundle deploy`. El bundle (`databricks.yml`) crea un job con tres
tareas encadenadas (features → train → score) en un cluster autoescalable, con
schedule diario. Secretos requeridos: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.

### Proyecto SQL → Snowflake
Push de un tag `sql-v*` dispara `cd-sql-snowflake.yml`: aplica las migraciones con
`schemachange`, despliega los stored procs de Snowpark ML, corre `dbt build` y
reentrena el modelo. Secretos requeridos: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`.

Los PR corren CI automáticamente (lint + tests + `dbt parse` / slim CI) sin tocar
producción.

---

## Optimizaciones incluidas

**PySpark:** AQE activado, `shuffle.partitions=64`, broadcast joins (umbral 32 MB),
serialización Kryo, compresión snappy, esquema RAW explícito (sin inferencia),
escritura particionada (Delta/Parquet), selección de *k* por silueta.

**Snowflake:** tres warehouses dedicados (LOAD/TRANSFORM/ML) para aislar cargas,
**cluster keys** + automatic clustering en las tablas grandes, RBAC de privilegio
mínimo (LOADER/TRANSFORMER/BI_READER), `SYSTEM$CLUSTERING_INFORMATION` para monitoreo,
y ML que corre **dentro de la base** (Snowpark) evitando egress de datos.

---

## Validación

| Métrica | PySpark | SQL (vía DuckDB) |
|---------|---------|------------------|
| Órdenes | 99.441 | 99.441 |
| Personas (`customer_unique_id`) | 96.096 | 96.096 |
| Clientes recurrentes | 2.997 | 2.997 |
| Segmentos generados | 5 | 5 |
| Tests | 6/6 ✅ | coverage ✅ |

La lógica de features de ambos proyectos fue cruzada sobre el CSV real y **coincide
exactamente**, garantizando que los dos motores son intercambiables.

---

## Licencia

MIT — ver [`LICENSE`](./LICENSE).
