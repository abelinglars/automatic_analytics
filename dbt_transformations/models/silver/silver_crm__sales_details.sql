select *
from {{ ref('bronze_crm__sales_details') }}
