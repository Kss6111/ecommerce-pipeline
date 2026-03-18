-- models/marts/mart_seller_performance.sql
-- Seller KPIs including revenue, delivery, and review scores

WITH seller_stats AS (
    SELECT
        s.SELLER_ID,
        s.city                                              AS seller_city,
        s.state                                             AS seller_state,
        COUNT(DISTINCT oi.ORDER_ID)                         AS total_orders,
        SUM(oi.ORDER_ITEM_ID)                               AS units_sold,
        ROUND(SUM(oi.price), 2)                             AS total_revenue,
        ROUND(AVG(oi.price), 2)                             AS avg_price,
        ROUND(AVG(o.actual_delivery_days), 1)               AS avg_delivery_days,
        ROUND(AVG(r.review_score), 2)                       AS avg_review_score,
        COUNT(CASE WHEN o.actual_delivery_days > o.estimated_delivery_days
                   THEN 1 END)                              AS late_deliveries,
        COUNT(DISTINCT oi.ORDER_ID) - COUNT(CASE WHEN o.actual_delivery_days > o.estimated_delivery_days
                   THEN 1 END)                              AS on_time_deliveries
    FROM {{ ref('stg_sellers') }} s
    JOIN {{ ref('stg_order_items') }} oi  ON s.SELLER_ID = oi.SELLER_ID
    JOIN {{ ref('stg_orders') }} o        ON oi.ORDER_ID = o.ORDER_ID
    LEFT JOIN {{ ref('stg_order_reviews') }} r ON o.ORDER_ID = r.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY 1, 2, 3
)

SELECT *,
    RANK() OVER (ORDER BY total_revenue DESC)       AS revenue_rank,
    RANK() OVER (ORDER BY avg_review_score DESC)    AS review_rank
FROM seller_stats
ORDER BY revenue_rank