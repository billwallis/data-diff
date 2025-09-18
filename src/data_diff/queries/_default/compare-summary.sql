select
    count(*) as records,
    {% for col in columns -%}
    countif({{ col }}__old is distinct from {{ col }}__new) as {{ col }}__mismatches{{ "" if loop.last else "," }}
    {% endfor %}
from temp_bill__compare
