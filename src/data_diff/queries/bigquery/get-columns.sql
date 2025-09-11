select
    column_name,
    ordinal_position,
    data_type,
from {{ database }}.{{ schema }}.INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ table }}'
