{% snapshot drivers_snapshot %}

{{
  config(
    target_schema=env_var('DATABRICKS_SCHEMA_SILVER', 'silver'),
    unique_key='driver_id',
    strategy='timestamp',
    updated_at="CAST(updated_at AS TIMESTAMP)",
  )
}}

SELECT * FROM {{ source('bronze', 'drivers') }}

{% endsnapshot %}
