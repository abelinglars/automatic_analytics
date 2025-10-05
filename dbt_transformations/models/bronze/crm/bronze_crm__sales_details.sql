with source as (
	
	select *
	from {{ source("crm", "crm_sales_details") }}

),
renamed_and_cleaned as (

	select
		trim(SLS_ORD_NUM) as order_number,
		trim(SLS_PRD_KEY) as product_key,
		SLS_CUST_ID as customer_id,
		{{ safe_date_from_int('SLS_ORDER_DT') }} as order_date,
		{{ safe_date_from_int('SLS_SHIP_DT') }} as ship_date,
		{{ safe_date_from_int('SLS_DUE_DT') }} as due_date,
		SLS_SALES as old_sales,
		case 
			when 
				SLS_SALES is null or
				SLS_SALES <= 0 or
				SLS_SALES != SLS_QUANTITY * abs(SLS_PRICE) 
			then SLS_QUANTITY * abs(SLS_PRICE)
			else SLS_SALES
		end as sales,
		SLS_QUANTITY as quantity,
		SLS_PRICE as old_price,
		case 
			when 
				SLS_PRICE <= 0 or
				SLS_PRICE is null 
			then SLS_SALES / nullif(SLS_QUANTITY, 0)
			else SLS_PRICE
		end as price,
		CURRENT_DATE() as dwh_created_at 
	from source

)

select * from renamed_and_cleaned
