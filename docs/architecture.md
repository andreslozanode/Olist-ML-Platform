# Arquitectura y decisiones de diseño

## Por qué este problema de ML
El dataset entregado es **solo la tabla `customers`** de Olist (una fila por orden).
La clave es que `customer_unique_id` identifica a la **persona**, mientras `customer_id`
es por orden. Eso permite construir una señal de **frecuencia/recurrencia** (RFM-lite)
sin necesitar la tabla de órdenes:

- 99.441 órdenes → **96.096 personas**
- **2.997 clientes recurrentes** (3,12 %)

Sobre esa base se aplica **K-Means** (no supervisado) con features de frecuencia +
geografía → segmentos accionables para marketing/logística.

## Patrón medallion (idéntico en ambos motores)
```
RAW  ─►  STAGING (limpieza, grano orden)
         └─► INTERMEDIATE (features, grano persona)  ──► baseline rule-based (SQL)
                 └─► MARTS (dim + fct + mart BI)  ◄── modelo K-Means
```

## Dos implementaciones, un contrato
| | Proyecto A (PySpark) | Proyecto B (SQL/Snowflake) |
|---|---|---|
| Transformación | PySpark DataFrame API | dbt (SQL) |
| Modelo | Spark ML `KMeans` + MLflow | Snowpark ML `KMeans` (in-DB) |
| Orquestación | Databricks Asset Bundle | Snowflake TASK + dbt |
| Optimización | AQE, broadcast, particionado | cluster keys, warehouses, ephemeral |
| Salida | Delta `gold.customer_segments` | `MART_BI_CUSTOMER_SEGMENTS` |

El **contrato de columnas es el mismo** (ver `bi_integration.md`), validado con un
cross-check: la lógica SQL reproduce exactamente los conteos del pipeline PySpark
(96.096 personas / 2.997 recurrentes / 99.441 órdenes).

## Decisiones clave
1. **Grano persona, no orden** — evita inflar la frecuencia y duplicar clientes en BI.
2. **Zip como string** — preserva ceros del CEP; el macro-región sale del 1er dígito.
3. **Baseline rule-based** en SQL — el pipeline nunca se rompe si el modelo ML aún no
   corrió; además sirve de control de calidad del modelo.
4. **Etiquetas de negocio derivadas del perfil del cluster** — estabilidad del
   dashboard entre reentrenamientos (los IDs de cluster son arbitrarios).
5. **RBAC con privilegio mínimo** — `BI_READER` solo lee marts.

## Extensión a Olist completo
Al incorporar `orders`, `order_items`, `payments`, `reviews`, el mismo esqueleto
soporta RFM real (Recency/Monetary), `value_score` y modelos supervisados (p.ej.
predicción de churn). Las tablas se enchufan en `staging/` e `intermediate/` sin tocar
la capa de marts ni la integración de BI.
