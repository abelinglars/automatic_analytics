with source as (

	select *
	from {{ source('erp', 'erp_loc_a101') }}

),

final as (

	select 
		replace(CID, '-', '') as customer_id,
		case 
			when trim(upper(CNTRY)) = 'DE' then 'Germany'
			when trim(upper(CNTRY)) in ('US', 'USA') then 'United states'
			when trim(upper(CNTRY)) = '' or CNTRY is null then 'n/a'
			else trim(CNTRY)
		end as country,
		CURRENT_DATE() as dwh_created_at
	from source

)

select * from final
