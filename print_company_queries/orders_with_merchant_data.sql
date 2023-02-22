/*
Write a query returning all ORDER_IDs with the time the merchant has been active at
the time of the order, the rank of the merchant by order count for the previous
month, and the merchant's primary sales channel for the previous month
*/

WITH monthly_orders AS ( -- calculating number of orders monthly for each merchant
	SELECT DATE_TRUNC('month', order_dt) AS month_
	, merchant_id
	, COUNT(DISTINCT order_id) AS orders_count
	FROM orders
	GROUP BY month_, merchant_id
)
, merchant_ranks AS ( -- assigning monthly ranks to merchants based on number of orders
	SELECT month_
	, merchant_id
	, orders_count
	, RANK() OVER (PARTITION BY month_ ORDER BY orders_count DESC) AS merchant_month_rank
	FROM monthly_orders
)
, primary_sales_channels AS ( -- selecting primary sales channel for each merchant per month
	SELECT DISTINCT ON (month_, merchant_id)
	DATE_TRUNC('month', order_dt) AS month_
	, merchant_id
	, sales_channel_type_id
	, COUNT(DISTINCT order_id) AS orders_count
	FROM orders
	GROUP BY month_, merchant_id, sales_channel_type_id
	ORDER BY month_, merchant_id, orders_count DESC
)
SELECT o.order_id
, o.merchant_id
, AGE(o.order_dt::date, o.merchant_registered_dt::date) AS merchant_active_for
, mr.merchant_month_rank AS prev_month_merchant_rank
, psc.sales_channel_type_id AS prev_month_primary_channel
FROM orders o
LEFT JOIN merchant_ranks mr ON o.merchant_id = mr.merchant_id 
 							AND DATE_TRUNC('month', o.order_dt) - INTERVAL '1' MONTH = mr.month_ -- taking merchant rank from previous month
LEFT JOIN primary_sales_channels psc ON o.merchant_id = psc.merchant_id
 									 AND DATE_TRUNC('month', o.order_dt) - INTERVAL '1' MONTH = psc.month_ -- taking sales channel from previous month
