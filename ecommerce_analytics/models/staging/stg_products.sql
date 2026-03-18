-- models/staging/stg_products.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_PRODUCTS') }}
),

renamed AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_CATEGORY_NAME,
        PRODUCT_NAME_LENGHT::INT    AS product_name_length,
        PRODUCT_DESCRIPTION_LENGHT::INT AS product_description_length,
        PRODUCT_PHOTOS_QTY::INT     AS photos_qty,
        PRODUCT_WEIGHT_G::FLOAT     AS weight_g,
        PRODUCT_LENGTH_CM::FLOAT    AS length_cm,
        PRODUCT_HEIGHT_CM::FLOAT    AS height_cm,
        PRODUCT_WIDTH_CM::FLOAT     AS width_cm
    FROM source
)

SELECT * FROM renamed