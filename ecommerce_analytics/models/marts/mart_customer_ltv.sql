-- models/marts/mart_customer_ltv.sql
-- Customer lifetime value with RFM scoring and segmentation

WITH customer_orders AS (
    SELECT
        o.CUSTOMER_ID,
        COUNT(DISTINCT o.ORDER_ID)                                          AS total_orders,
        ROUND(SUM(p.payment_value), 2)                                      AS total_spent,
        ROUND(AVG(p.payment_value), 2)                                      AS avg_order_value,
        MIN(o.purchased_at)                                                 AS first_order_date,
        MAX(o.purchased_at)                                                 AS last_order_date,
        DATEDIFF('day', MIN(o.purchased_at), MAX(o.purchased_at))           AS customer_lifespan_days
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_payments') }} p ON o.ORDER_ID = p.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY 1
),

rfm AS (
    SELECT *,
        DATEDIFF('day', last_order_date, CURRENT_DATE())                    AS recency_days,
        NTILE(5) OVER (ORDER BY DATEDIFF('day', last_order_date, CURRENT_DATE()) ASC)  AS recency_score,
        NTILE(5) OVER (ORDER BY total_orders DESC)                          AS frequency_score,
        NTILE(5) OVER (ORDER BY total_spent DESC)                           AS monetary_score
    FROM customer_orders
),

segmented AS (
    SELECT *,
        (recency_score + frequency_score + monetary_score) AS rfm_total,
        CASE
            WHEN (recency_score + frequency_score + monetary_score) >= 13 THEN 'Champions'
            WHEN (recency_score + frequency_score + monetary_score) >= 10 THEN 'Loyal Customers'
            WHEN (recency_score + frequency_score + monetary_score) >= 7  THEN 'Potential Loyalists'
            WHEN (recency_score + frequency_score + monetary_score) >= 4  THEN 'At Risk'
            ELSE 'Lost'
        END AS customer_segment
    FROM rfm
)

SELECT * FROM segmented