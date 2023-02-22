-- Write a query returning total sales, orders, and count of merchants by month

SELECT DATE_TRUNC('month', order_dt)::date AS month_
, ROUND(SUM(total_cost)::numeric, 2) AS total_sales
, COUNT(DISTINCT order_id) AS total_orders
, COUNT(DISTINCT merchant_id) AS merchants_count
FROM orders
GROUP BY month_
