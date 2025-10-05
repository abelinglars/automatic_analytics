select *
from {{ ref('bronze_crm__customer_info') }}
