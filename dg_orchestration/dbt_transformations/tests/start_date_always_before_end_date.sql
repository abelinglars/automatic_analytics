select *
from {{ ref('bronze_crm__product_info') }}
where start_date > end_date

