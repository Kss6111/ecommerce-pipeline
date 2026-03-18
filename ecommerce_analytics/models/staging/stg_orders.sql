-- models/staging/stg_orders.sql
-- Cleans and renames the raw orders table

WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDERS') }}
),

renamed AS (
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_STATUS,
        TRY_TO_TIMESTAMP(ORDER_PURCHASE_TIMESTAMP)      AS purchased_at,
        TRY_TO_TIMESTAMP(ORDER_APPROVED_AT)             AS approved_at,
        TRY_TO_TIMESTAMP(ORDER_DELIVERED_CARRIER_DATE)  AS shipped_at,
        TRY_TO_TIMESTAMP(ORDER_DELIVERED_CUSTOMER_DATE) AS delivered_at,
        TRY_TO_TIMESTAMP(ORDER_ESTIMATED_DELIVERY_DATE) AS estimated_delivery_at,
        DATEDIFF('day', ORDER_PURCHASE_TIMESTAMP, ORDER_DELIVERED_CUSTOMER_DATE)  AS actual_delivery_days,
        DATEDIFF('day', ORDER_PURCHASE_TIMESTAMP, ORDER_ESTIMATED_DELIVERY_DATE)  AS estimated_delivery_days
    FROM source
    WHERE ORDER_STATUS IS NOT NULL
)

SELECT * FROM renamed