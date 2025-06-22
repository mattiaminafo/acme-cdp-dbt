{{ config(
    materialized = 'incremental',                    -- tabelle fisiche
    partition_by = {                                 -- partiziona per giorno
        'field': 'order_date',
        'data_type': 'date'
    },
    incremental_strategy = 'insert_overwrite'        -- riscrivi solo le partizioni recenti
) }}

WITH base AS (

    SELECT
        DATE(updated_at)       AS order_date,        -- chiave di partizione
        id                     AS order_id,          -- ← usa la colonna “id” di Shopify
        email,
        total_price            AS amount_usd,
        updated_at
    FROM {{ source('raw_shopify', 'orders') }}

    {% if is_incremental() %}
      -- aggiorno solo gli ultimi 7 giorni quando il modello esiste già
      WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    {% endif %}

)

SELECT *
FROM base
