with source as (

	select *
	from {{ source('erp', 'erp_cust_az12') }}

),

renamed_and_cleaned as (

	select 
		case
			when CID like 'NAS%' then substring(CID, 4, len(CID))
			else CID
		end as customer_id,
		case
			when BDATE > CURRENT_DATE() then null
			else BDATE 
		end as birthday,
		case 
			when trim(upper(GEN)) in ('F', 'FEMALE') then 'female'
			when trim(upper(GEN)) in ('M', 'MALE') then 'male'
			else 'n/a'
		end as gender,
		CURRENT_DATE() as dwh_created_at
	from source
)

select * from renamed_and_cleaned
