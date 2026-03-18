-- models/marts/mart_product_performance.sql
-- Product and category performance with ranking

WITH product_sales AS (
    SELECT
        p.PRODUCT_ID,
        COALESCE(t.PRODUCT_CATEGORY_NAME_ENGLISH, 'Unknown') AS category,
        COUNT(DISTINCT oi.ORDER_ID)                           AS total_orders,
        SUM(oi.ORDER_ITEM_ID)                                 AS units_sold,
        ROUND(SUM(oi.price), 2)                               AS total_revenue,
        ROUND(AVG(oi.price), 2)                               AS avg_price,
        ROUND(AVG(r.review_score), 2)                         AS avg_review_score,
        COUNT(r.REVIEW_ID)                                    AS total_reviews
    FROM {{ ref('stg_products') }} p
    JOIN {{ ref('stg_order_items') }} oi      ON p.PRODUCT_ID = oi.PRODUCT_ID
    JOIN {{ ref('stg_orders') }} o            ON oi.ORDER_ID = o.ORDER_ID
    LEFT JOIN {{ ref('stg_category_translation') }} t ON p.PRODUCT_CATEGORY_NAME = t.PRODUCT_CATEGORY_NAME
    LEFT JOIN {{ ref('stg_order_reviews') }} r        ON o.ORDER_ID = r.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY 1, 2
),

ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY total_revenue DESC)                       AS revenue_rank,
        RANK() OVER (ORDER BY avg_review_score DESC)                    AS review_rank,
        RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rank_in_category
    FROM product_sales
)

SELECT * FROM ranked ORDER BY revenue_rank