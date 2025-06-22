-- models/stg_shopify_orders.sql
{{ config(materialized='incremental',
          partition_by={'field': 'order_date', 'data_type': 'date'},
          incremental_strategy='insert_overwrite') }}

WITH base AS (
  SELECT
    DATE(updated_at)         AS order_date,
    order_id,
    email,
    total_price            AS amount_usd,
    updated_at
  FROM {{ source('raw_shopify', 'orders') }}
  {% if is_incremental() %}
    WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  {% endif %}
)

SELECT * FROM base;
