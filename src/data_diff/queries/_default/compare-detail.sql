select *
from temp_bill__compare
where 0=1
    {% for col in columns -%}
    or {{ col }}__old is distinct from {{ col }}__new
    {% endfor %}
order by {{ primary_keys | join(", ") }}
limit 100
