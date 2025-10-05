select *
from {{ ref('bronze_crm__sales_details') }}
where
	order_date > ship_date or order_date > due_date
