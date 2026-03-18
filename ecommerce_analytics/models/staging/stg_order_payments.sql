-- models/staging/stg_order_payments.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDER_PAYMENTS') }}
),

renamed AS (
    SELECT
        ORDER_ID,
        PAYMENT_SEQUENTIAL,
        PAYMENT_TYPE,
        PAYMENT_INSTALLMENTS::INT   AS payment_installments,
        PAYMENT_VALUE::FLOAT        AS payment_value
    FROM source
    WHERE PAYMENT_VALUE IS NOT NULL
)

SELECT * FROM renamed