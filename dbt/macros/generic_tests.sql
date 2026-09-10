{% test no_orphan_rows(model, column_name, parent_model, parent_column) %}
  select
    child.{{ column_name }} as orphan_key
  from {{ model }} as child
  left join {{ parent_model }} as parent
    on child.{{ column_name }} = parent.{{ parent_column }}
  where child.{{ column_name }} is not null
    and parent.{{ parent_column }} is null
{% endtest %}

{% test accepted_range(model, column_name, min_value, max_value) %}
  select
    {{ column_name }} as out_of_range_value
  from {{ model }}
  where {{ column_name }} is not null
    and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}
