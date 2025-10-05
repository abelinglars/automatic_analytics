{% macro safe_date_from_int(col_name) %}
case
	when {{ col_name }} = 0 or len({{ col_name }}) != 8 then null
	else to_date(to_varchar({{ col_name }}), 'YYYYMMDD')
end
{% endmacro %}
