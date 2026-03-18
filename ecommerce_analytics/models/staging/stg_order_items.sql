-- models/staging/stg_order_items.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDER_ITEMS') }}
),

renamed AS (
    SELECT
        ORDER_ID,
        ORDER_ITEM_ID,
        PRODUCT_ID,
        SELLER_ID,
        TRY_TO_TIMESTAMP(SHIPPING_LIMIT_DATE) AS shipping_limit_at,
        PRICE::FLOAT                          AS price,
        FREIGHT_VALUE::FLOAT                  AS freight_value,
        (PRICE + FREIGHT_VALUE)::FLOAT        AS total_item_value
    FROM source
)

SELECT * FROM renamed