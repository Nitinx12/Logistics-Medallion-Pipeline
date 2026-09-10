{% snapshot vehicles_snapshot %}

{{
  config(
    target_schema=env_var('DATABRICKS_SCHEMA_SILVER', 'silver'),
    unique_key='truck_id',
    strategy='timestamp',
    updated_at='updated_at',
  )
}}

{% if target.type == 'duckdb' %}
SELECT * REPLACE (CAST(updated_at AS TIMESTAMP) AS updated_at) FROM {{ source('bronze', 'trucks') }}
{% else %}
SELECT * FROM {{ source('bronze', 'trucks') }}
{% endif %}

{% endsnapshot %}
