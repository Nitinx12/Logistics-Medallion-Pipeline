{% macro incremental_filter(col='updated_at') %}
  {% if is_incremental() %}
  where {{ col }}::timestamp > (
      select coalesce(max({{ col }}), '1900-01-01'::timestamp) from {{ this }}
  )
  {% endif %}
{% endmacro %}

{% macro dedup_by_key(key) %}
  row_number() over (
      partition by {{ key }}
      order by updated_at desc
  ) as row_num
{% endmacro %}
