SELECT
    zone,
    ROUND(AVG(CASE WHEN status = 'success' THEN delivery_duration_mins ELSE NULL END), 2) AS avg_delivery_time_mins,
    ROUND(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) / COUNT(delivery_id), 4) AS failure_rate,
    COUNT(delivery_id) AS total_attempts
FROM
    silver_delivery_logs
GROUP BY
    zone
ORDER BY
    zone;
