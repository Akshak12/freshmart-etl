SELECT
    CAST(order_date AS DATE) AS order_date,
    city,
    ROUND(SUM(order_total), 2) AS total_revenue,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(order_total) / COUNT(order_id), 2) AS avg_basket_size
FROM
    silver_orders
WHERE
    status = 'delivered'
GROUP BY
    CAST(order_date AS DATE),
    city
ORDER BY
    order_date,
    city;
