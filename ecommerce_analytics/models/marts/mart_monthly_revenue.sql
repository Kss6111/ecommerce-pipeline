-- models/marts/mart_monthly_revenue.sql
-- Monthly revenue trends with growth and rolling averages

WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', o.purchased_at)    AS order_month,
        COUNT(DISTINCT o.ORDER_ID)             AS total_orders,
        COUNT(DISTINCT o.CUSTOMER_ID)          AS unique_customers,
        ROUND(SUM(p.payment_value), 2)         AS total_revenue,
        ROUND(AVG(p.payment_value), 2)         AS avg_order_value
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_payments') }} p ON o.ORDER_ID = p.ORDER_ID
    WHERE o.ORDER_STATUS = 'delivered'
    GROUP BY 1
),

with_growth AS (
    SELECT *,
        LAG(total_revenue) OVER (ORDER BY order_month) AS prev_month_revenue,
        ROUND(100.0 * (total_revenue - LAG(total_revenue) OVER (ORDER BY order_month))
            / NULLIF(LAG(total_revenue) OVER (ORDER BY order_month), 0), 2) AS revenue_growth_pct,
        SUM(total_revenue) OVER (ORDER BY order_month)  AS cumulative_revenue,
        AVG(total_revenue) OVER (ORDER BY order_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)   AS rolling_3mo_avg
    FROM monthly_sales
)

SELECT * FROM with_growth ORDER BY order_month