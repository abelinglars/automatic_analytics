select *
from {{ ref('bronze_crm__product_info')}}
where cost <= 0
