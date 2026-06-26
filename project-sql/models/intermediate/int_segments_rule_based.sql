-- Baseline de segmentación 100% SQL (auditable), sin dependencia de ML.
-- Sirve de fallback y de validación contra el modelo K-Means de Snowpark ML.
with f as (
    select * from {{ ref('int_customer_features') }}
)
select
    customer_unique_id,
    case
        when num_orders >= 3                       then 'Recurrente alto'
        when is_repeat_customer = 1                then 'Recurrente'
        when num_distinct_states > 1               then 'Itinerante'
        when state_market_share  > 0.30            then 'Metropolitano'
        else 'Estándar regional'
    end as segment_label_rule
from f
