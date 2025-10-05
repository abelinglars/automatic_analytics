select 
	ID as id,
	CAT as category,
	SUBCAT as sub_category,
	MAINTENANCE as maintenanc,
	CURRENT_DATE() as dwh_created_at
from {{ source('erp', 'erp_px_cat_g1v2') }}
