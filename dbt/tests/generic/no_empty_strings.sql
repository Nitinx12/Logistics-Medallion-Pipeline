{% test no_empty_strings(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE CAST({{ column_name }} AS STRING) = ''

{% endtest %}
