-- models/staging/stg_category_translation.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_CATEGORY_TRANSLATION') }}
),

renamed AS (
    SELECT
        PRODUCT_CATEGORY_NAME,
        PRODUCT_CATEGORY_NAME_ENGLISH
    FROM source
)

SELECT * FROM renamed