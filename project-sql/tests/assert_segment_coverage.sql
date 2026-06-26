-- Test singular: toda persona en la dimensión debe tener segmento asignado.
select d.customer_unique_id
from {{ ref('dim_customers') }} d
left join {{ ref('fct_customer_segments') }} s
    on d.customer_unique_id = s.customer_unique_id
where s.customer_unique_id is null
