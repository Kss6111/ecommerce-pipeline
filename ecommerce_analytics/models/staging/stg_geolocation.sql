-- models/staging/stg_geolocation.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_GEOLOCATION') }}
),

renamed AS (
    SELECT
        GEOLOCATION_ZIP_CODE_PREFIX AS zip_code,
        GEOLOCATION_LAT::FLOAT      AS latitude,
        GEOLOCATION_LNG::FLOAT      AS longitude,
        GEOLOCATION_CITY            AS city,
        GEOLOCATION_STATE           AS state
    FROM source
)

SELECT * FROM renamed