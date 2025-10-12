with source as (

	select * 
	from {{ source('dagster', 'crm_cust_info') }}

),

renamed_and_cleaned as (

	select
		CST_ID as customer_id,
		trim(CST_KEY) as customer_key,
		trim(CST_FIRSTNAME) as first_name,
		trim(CST_LASTNAME) as last_name,
		case 
			when trim(upper(CST_MARITAL_STATUS)) = 'S' then 'single'
			when trim(upper(CST_MARITAL_STATUS)) = 'M' then 'married'
			else 'n/a'
		end as marital_status,
		case
			when trim(upper(CST_GNDR)) = 'M' then 'male'
			when trim(upper(CST_GNDR)) = 'F' then 'female'
			else 'n/a'
		end as gender,
		date(CST_CREATE_DATE) as create_date,
		CURRENT_DATE() as dwh_created_at
	from source

),

find_duplicates as (

	select 
		*,
		row_number() over(partition by customer_id order by create_date desc) as rank_freshness
	from renamed_and_cleaned

),

remove_duplicates as (

	select
		customer_id,
		customer_key,
		first_name,
		last_name,
		marital_status,
		gender,
		create_date,
		dwh_created_at
	from find_duplicates
	where 
		rank_freshness = 1 and 
		customer_id is not null

)

select * from remove_duplicates
