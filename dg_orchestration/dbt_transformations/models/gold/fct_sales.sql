select
	sa.order_number,
	cu.customer_surrogate as customer_key,
	po.product_surrogate as product_key,
	--sa.product_key,
	-- sa.customer_id,
	sa.order_date,
	sa.ship_date,
	sa.due_date,
	sa.sales,
	sa.quantity,
	sa.price
from {{ ref('silver_crm__sales_details') }} as sa
join {{ ref('dim_customers') }} as cu
on
	sa.customer_id = cu.customer_id
join {{ ref('dim_products') }} as po
on 
	sa.product_key = po.product_key
