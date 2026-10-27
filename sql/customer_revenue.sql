SELECT
    c.customer_id,
    c.customer_name,
    c.region,
    SUM(o.quantity * o.unit_price) AS total_revenue
FROM orders AS o
INNER JOIN customers AS c
    ON c.customer_id = o.customer_id
WHERE o.status = 'COMPLETED'
  AND o.customer_id IS NOT NULL
  AND o.quantity IS NOT NULL
  AND o.quantity > 0
  AND o.unit_price IS NOT NULL
  AND o.unit_price > 0
GROUP BY
    c.customer_id,
    c.customer_name,
    c.region
ORDER BY total_revenue DESC;