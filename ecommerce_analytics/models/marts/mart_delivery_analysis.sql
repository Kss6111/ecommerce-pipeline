-- models/marts/mart_delivery_analysis.sql
-- Delivery time analysis by state and on-time performance

WITH delivery_stats AS (
    SELECT
        o.ORDER_ID,
        c.state                                                         AS customer_state,
        s.state                                                         AS seller_state,
        o.actual_delivery_days,
        o.estimated_delivery_days,
        o.actual_delivery_days - o.estimated_delivery_days             AS delivery_delay_days,
        CASE
            WHEN o.actual_delivery_days <= o.estimated_delivery_days THEN 'On Time'
            ELSE 'Late'
        END                                                             AS delivery_status,
        r.review_score
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c       ON o.CUSTOMER_ID = c.CUSTOMER_ID
    JOIN {{ ref('stg_order_items') }} oi    ON o.ORDER_ID = oi.ORDER_ID
    JOIN {{ ref('stg_sellers') }} s         ON oi.SELLER_ID = s.SELLER_ID
    LEFT JOIN {{ ref('stg_order_reviews') }} r ON o.ORDER_ID = r.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
      AND o.actual_delivery_days IS NOT NULL
)

SELECT
    customer_state,
    COUNT(DISTINCT ORDER_ID)                    AS total_orders,
    ROUND(AVG(actual_delivery_days), 1)         AS avg_delivery_days,
    ROUND(AVG(delivery_delay_days), 1)          AS avg_delay_days,
    COUNT(CASE WHEN delivery_status = 'On Time' THEN 1 END) AS on_time_orders,
    COUNT(CASE WHEN delivery_status = 'Late' THEN 1 END)    AS late_orders,
    ROUND(100.0 * COUNT(CASE WHEN delivery_status = 'On Time' THEN 1 END)
        / COUNT(DISTINCT ORDER_ID), 1)          AS on_time_pct,
    ROUND(AVG(review_score), 2)                 AS avg_review_score
FROM delivery_stats
GROUP BY 1
ORDER BY total_orders DESC