select
    column_name,
    ordinal_position,
    data_type
from {{ database }}.INFORMATION_SCHEMA.COLUMNS
where 1=1
    and table_schema = '{{ schema }}'
    and table_name = '{{ table }}'
