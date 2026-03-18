-- models/staging/stg_sellers.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_SELLERS') }}
),

renamed AS (
    SELECT
        SELLER_ID,
        SELLER_ZIP_CODE_PREFIX AS zip_code,
        SELLER_CITY            AS city,
        SELLER_STATE           AS state
    FROM source
)

SELECT * FROM renamed