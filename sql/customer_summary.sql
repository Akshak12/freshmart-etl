WITH orders_agg AS (
    SELECT
        customer_id,
        ROUND(SUM(order_total), 2) AS total_spend,
        COUNT(order_id) AS order_count,
        MAX(order_date) AS last_order_date
    FROM
        silver_orders
    WHERE
        status = 'delivered'
    GROUP BY
        customer_id
)
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.phone,
    c.city,
    c.registered_on,
    c.loyalty_points,
    COALESCE(oa.total_spend, 0.0) AS total_spend,
    COALESCE(oa.order_count, 0) AS order_count,
    oa.last_order_date
FROM
    silver_customers c
LEFT JOIN
    orders_agg oa
ON
    c.customer_id = oa.customer_id
ORDER BY
    total_spend DESC;
