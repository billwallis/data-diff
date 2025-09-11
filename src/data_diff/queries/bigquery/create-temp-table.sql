create temporary table temp_bill__compare as
    select
        {{ primary_keys | join(', ') }},
        {% for col in columns -%}
        old_.{{ col }} as {{ col }}__old,
        new_.{{ col }} as {{ col }}__new,
        {% endfor %}
    from {{ identifier_1 }} as old_
        full join {{ identifier_2 }} as new_
            using ({{ primary_keys | join(', ') }})
