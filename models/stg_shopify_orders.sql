-- models/stg_shopify_orders.sql
{{
    config(
        materialized="incremental",
        partition_by={"field": "order_date", "data_type": "date"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    base as (
        select
            date(updated_at) as order_date,
            order_id,
            email,
            total_price as amount_usd,
            updated_at
        from {{ source("raw_shopify", "orders") }}
        {% if is_incremental() %}
            where updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
        {% endif %}
    )

select *
from base
