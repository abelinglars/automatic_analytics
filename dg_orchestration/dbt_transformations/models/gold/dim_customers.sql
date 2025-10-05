select
	crm_customer.customer_id,
	row_number() over(order by crm_customer.customer_id) as customer_surrogate,
	crm_customer.customer_key,
	crm_customer.first_name,
	crm_customer.last_name,
	case
		when crm_customer.gender != 'n/a' then crm_customer.gender
		else coalesce(erp_customer.gender, 'n/a')
	end as gender,
	location.country,
	crm_customer.marital_status,
	erp_customer.birthday,
	crm_customer.create_date
from {{ ref('silver_crm__customer_info') }} as crm_customer
join {{ ref('silver_erp__location') }} as location
on
	crm_customer.customer_key = location.customer_id
join {{ ref('silver_erp__customer_info')}} as erp_customer
	on crm_customer.customer_key = erp_customer.customer_id
