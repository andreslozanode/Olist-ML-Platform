-- Dimensión de clientes (grano persona). Cluster key por región para pruning en BI.
{{ config(materialized='table', cluster_by=['primary_region']) }}

select
    customer_unique_id,
    primary_customer_id,
    primary_state,
    primary_region,
    region_idx,
    zip_macro_region,
    num_orders,
    is_repeat_customer,
    num_distinct_states,
    num_distinct_cities,
    state_market_share
from {{ ref('int_customer_features') }}
