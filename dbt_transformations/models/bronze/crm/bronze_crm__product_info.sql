with source as (

	select * 
	from {{ source('crm', 'crm_prd_info') }}

),

renamed as (

	select
		PRD_ID as product_id,
		trim(PRD_KEY) as composite_product_key,
		trim(PRD_NM) as name,
		PRD_COST as cost,
		trim(PRD_LINE) as line,
		date(PRD_START_DT) as start_date,
		date(PRD_END_DT) as end_date,
		CURRENT_DATE() as dwh_created_at
	from source

),

cleaned as (

	select 
		product_id,
		composite_product_key,
		replace(substring(composite_product_key, 1, 5), '-', '_' ) as product_category,
		substring(composite_product_key, 7, len(composite_product_key)) as product_key,
		name,
		cost,
		case upper(line) 
			when 'R' then 'Road'
			when 'M' then 'Mountain'
			when 'S' then 'Other sales'
			when 'T' then 'Touring'
			else 'n/a'
		end as line,
		start_date,
		lead(start_date) over(partition by composite_product_key order by start_date) -1 as end_date,
		dwh_created_at
	from renamed
)

select * from cleaned
