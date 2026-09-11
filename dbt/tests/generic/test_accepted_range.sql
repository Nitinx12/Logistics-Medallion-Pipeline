{% test accepted_range(model, column_name, min_value=none, max_value=none) %}

{% if min_value is none and max_value is none %}
  {{ exceptions.raise_compiler_error("accepted_range: at least one of min_value or max_value must be provided") }}
{% endif %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (
    {% if min_value is not none %} try_cast({{ column_name }} AS DOUBLE) < {{ min_value }} {% endif %}
    {% if min_value is not none and max_value is not none %} OR {% endif %}
    {% if max_value is not none %} try_cast({{ column_name }} AS DOUBLE) > {{ max_value }} {% endif %}
  )

{% endtest %}