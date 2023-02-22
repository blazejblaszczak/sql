-- Write a query returning merchants total sales, product count, and order count ordered by order count for merchants with more than 5 orders

SELECT o.merchant_id
, ROUND(SUM(o.total_cost)::numeric, 2) AS total_sales
, SUM(i.quantity) AS products_count
, COUNT(DISTINCT o.order_id) AS total_orders
FROM orders o
JOIN items i ON o.order_id = i.order_id
GROUP BY o.merchant_id
HAVING COUNT(DISTINCT o.order_id) > 5 -- filtering merchants with more than 5 orders
ORDER BY total_orders DESC
