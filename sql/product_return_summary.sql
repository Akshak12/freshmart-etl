SELECT
    i.product_id,
    i.product_name,
    i.category,
    SUM(CASE WHEN o.status = 'returned' THEN i.qty ELSE 0 END) AS return_count,
    SUM(i.qty) AS total_sold_count,
    ROUND(SUM(CASE WHEN o.status = 'returned' THEN i.qty ELSE 0 END) / SUM(i.qty), 4) AS return_rate
FROM
    silver_order_items i
INNER JOIN
    silver_orders o
ON
    i.order_id = o.order_id
GROUP BY
    i.product_id,
    i.product_name,
    i.category
ORDER BY
    return_count DESC;
