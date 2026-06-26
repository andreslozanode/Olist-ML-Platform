-- Limpieza y tipado. El zip se conserva como string (preserva ceros del CEP).
with source as (
    select * from {{ source('raw', 'olist_customers') }}
),

cleaned as (
    select
        customer_id,
        customer_unique_id,
        lpad(trim(customer_zip_code_prefix), 5, '0')        as zip_code_prefix,
        try_to_number(left(trim(customer_zip_code_prefix), 1)) as zip_macro_region,
        initcap(trim(customer_city))                        as customer_city,
        upper(trim(customer_state))                         as customer_state
    from source
    where customer_id is not null
      and customer_unique_id is not null
)

select * from cleaned
