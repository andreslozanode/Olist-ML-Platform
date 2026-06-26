-- Mart ANCHA lista para BI (Tableau / Power BI / Looker). Una fila por persona,
-- con dimensiones + métricas + segmento. Cluster key: (region, segment).
{{ config(
    materialized='table',
    cluster_by=['primary_region', 'segment_id']
) }}

select
    d.customer_unique_id,
    d.primary_customer_id,
    d.primary_state,
    d.primary_region,
    d.zip_macro_region,
    d.num_orders,
    d.is_repeat_customer,
    d.num_distinct_states,
    d.num_distinct_cities,
    round(d.state_market_share, 4)  as state_market_share,
    s.segment_id,
    s.segment_label,
    s.segment_source,
    s.model_version,
    s.scored_at
from {{ ref('dim_customers') }} d
inner join {{ ref('fct_customer_segments') }} s
    on d.customer_unique_id = s.customer_unique_id
