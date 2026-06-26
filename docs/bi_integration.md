# Integración con BI / Reporting

Ambos proyectos convergen en un **contrato de datos único** a grano persona, de modo
que el dashboard es idéntico independientemente del motor que lo produjo.

## Contrato de salida (tabla/archivo `customer_segments`)

| Columna | Tipo | Descripción |
|---|---|---|
| `customer_unique_id` | string | Clave de persona (grano) |
| `primary_customer_id` | string | customer_id representativo |
| `num_orders` | int | Frecuencia (proxy RFM) |
| `is_repeat_customer` | int | 1 = recurrente |
| `num_distinct_states` / `num_distinct_cities` | int | Movilidad geográfica |
| `zip_macro_region` | int | Macro-región CEP (0–9) |
| `primary_state` | string | Estado dominante (UF) |
| `primary_region` | string | Región IBGE |
| `state_market_share` | float | Peso del estado en la base |
| `segment_id` | int | Cluster K-Means |
| `segment_label` | string | Etiqueta de negocio |
| `model_version` | string | Versión del modelo |
| `scored_at` | timestamp | Marca temporal del scoring |

## Cómo se conecta cada herramienta

**Power BI / Tableau / Looker → Snowflake (Proyecto B)**
Conectar con el rol `BI_READER` (solo lectura) a `ANALYTICS.MARTS.MART_BI_CUSTOMER_SEGMENTS`.
Los *cluster keys* `(primary_region, segment_id)` aceleran los filtros más comunes
(por región y por segmento). Para Tableau/Power BI usar **DirectQuery/Live** sobre la
mart; para Looker, apuntar el `explore` a esa tabla.

**Power BI / Tableau → Databricks (Proyecto A)**
Conectar vía el conector nativo de Databricks (SQL Warehouse) a la tabla Delta
`gold.customer_segments`, particionada por `primary_region`. El particionamiento
permite *partition pruning* en filtros por región.

## Métricas/visuales sugeridos
- Distribución de clientes por `segment_label` (barras).
- Mapa coroplético por `primary_state` / `primary_region`.
- % de recurrentes (`is_repeat_customer`) por región.
- Tabla de `state_market_share` para priorización comercial.

## Notas de gobernanza
- `model_version` y `scored_at` permiten auditar qué versión alimenta cada reporte.
- `segment_source` (en el Proyecto B) indica si el segmento vino del modelo K-Means o
  del baseline SQL, útil para validar el despliegue del modelo.
