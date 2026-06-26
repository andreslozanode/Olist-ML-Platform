-- Features a grano PERSONA (customer_unique_id). Mismo contrato que el pipeline PySpark.
with orders as (
    select * from {{ ref('stg_customers') }}
),

region_map as (
    select * from {{ ref('br_state_region') }}
),

state_share as (
    select
        customer_state,
        count(*) / sum(count(*)) over () as state_market_share
    from orders
    group by customer_state
),

-- estado dominante por persona (mayor frecuencia, desempate alfabético)
primary_state as (
    select customer_unique_id, customer_state as primary_state
    from (
        select
            customer_unique_id,
            customer_state,
            row_number() over (
                partition by customer_unique_id
                order by count(*) desc, customer_state asc
            ) as rn
        from orders
        group by customer_unique_id, customer_state
    )
    where rn = 1
),

agg as (
    select
        customer_unique_id,
        any_value(customer_id)              as primary_customer_id,
        count(*)                            as num_orders,
        iff(count(*) > 1, 1, 0)             as is_repeat_customer,
        count(distinct customer_state)      as num_distinct_states,
        count(distinct customer_city)       as num_distinct_cities,
        max(zip_macro_region)               as zip_macro_region
    from orders
    group by customer_unique_id
)

select
    a.customer_unique_id,
    a.primary_customer_id,
    a.num_orders,
    a.is_repeat_customer,
    a.num_distinct_states,
    a.num_distinct_cities,
    coalesce(a.zip_macro_region, -1)        as zip_macro_region,
    p.primary_state,
    r.region                                as primary_region,
    r.region_idx,
    coalesce(s.state_market_share, 0)       as state_market_share
from agg a
left join primary_state p on a.customer_unique_id = p.customer_unique_id
left join region_map    r on p.primary_state      = r.state_code
left join state_share   s on p.primary_state      = s.customer_state
