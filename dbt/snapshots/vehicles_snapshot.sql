{% snapshot vehicles_snapshot %}

{{
  config(
    target_schema=env_var('DATABRICKS_SCHEMA_SILVER', 'silver'),
    unique_key='truck_id',
    strategy='timestamp',
    updated_at="CAST(updated_at AS TIMESTAMP)",
  )
}}

SELECT * FROM {{ source('bronze', 'trucks') }}

{% endsnapshot %}
