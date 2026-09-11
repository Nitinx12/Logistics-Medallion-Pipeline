{% test no_orphan_rows(model, column_name, parent_model, parent_column) %}

SELECT
    child.{{ column_name }} AS orphan_value
FROM {{ model }} AS child
LEFT JOIN {{ parent_model }} AS parent
    ON child.{{ column_name }} = parent.{{ parent_column }}
WHERE child.{{ column_name }} IS NOT NULL
  AND parent.{{ parent_column }} IS NULL

{% endtest %}
