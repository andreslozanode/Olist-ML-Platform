-- Hechos de segmentación. Combina el modelo ML (Snowpark K-Means) con el baseline SQL.
-- Si la tabla ML aún no existe (primera corrida), cae al segmento rule-based.
{{ config(materialized='table', cluster_by=['segment_id']) }}

with features as (
    select * from {{ ref('int_customer_features') }}
),

rule_based as (
    select * from {{ ref('int_segments_rule_based') }}
),

ml as (
    {% if adapter.get_relation(database='ANALYTICS', schema='ML', identifier='CUSTOMER_SEGMENTS_ML') %}
    select customer_unique_id, segment_id, segment_label
    from {{ var('ml_segments_relation') }}
    {% else %}
    select
        null::varchar as customer_unique_id,
        null::int     as segment_id,
        null::varchar as segment_label
    where false
    {% endif %}
)

select
    f.customer_unique_id,
    f.primary_customer_id,
    coalesce(m.segment_id, -1)                              as segment_id,
    coalesce(m.segment_label, rb.segment_label_rule)        as segment_label,
    iff(m.segment_id is not null, 'kmeans_snowpark', 'rule_based') as segment_source,
    '{{ var("model_version") }}'                            as model_version,
    current_timestamp()                                    as scored_at
from features f
left join rule_based rb on f.customer_unique_id = rb.customer_unique_id
left join ml          m  on f.customer_unique_id = m.customer_unique_id
