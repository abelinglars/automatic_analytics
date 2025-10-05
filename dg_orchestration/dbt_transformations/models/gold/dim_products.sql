select 
	crm_product.product_id,
	row_number() over(order by crm_product.start_date, crm_product.product_key) as product_surrogate,
	crm_product.product_category as category_key,
	crm_product.product_key,
	crm_product.name,
	crm_product.cost,
	crm_product.line,
	product_category.category,
	product_category.sub_category,
	product_category.maintenance
from {{ ref('silver_crm__product_info')}} as crm_product
join {{ ref('silver_erp__product_category') }} as product_category
on
	crm_product.product_category = product_category.id
where crm_product.end_date is null -- only keep latest
