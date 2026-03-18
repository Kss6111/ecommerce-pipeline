-- models/staging/stg_order_reviews.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDER_REVIEWS') }}
),

renamed AS (
    SELECT
        REVIEW_ID,
        ORDER_ID,
        REVIEW_SCORE::INT                         AS review_score,
        REVIEW_COMMENT_TITLE                      AS review_title,
        REVIEW_COMMENT_MESSAGE                    AS review_message,
        TRY_TO_TIMESTAMP(REVIEW_CREATION_DATE)    AS review_created_at,
        TRY_TO_TIMESTAMP(REVIEW_ANSWER_TIMESTAMP) AS review_answered_at
    FROM source
)

SELECT * FROM renamed