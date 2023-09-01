WITH stocks_data AS (
    SELECT DATE_TRUNC({{period}}, filled_at) AS date_
    , i.symbol
    , i.company_name
    , COUNT(o.id) AS orders_count
    , SUM(o.order_value) AS total_value
    FROM orders o
    JOIN instruments i ON o.instrument_id = i.id
    WHERE TRUE
    AND o.filled_at BETWEEN {{start_date}} AND {{end_date}} + 1
    AND o.side = lower({{buy_or_sell}})
    GROUP BY date_, i.symbol, i.company_name
)
SELECT s.*
FROM (
SELECT *
, RANK() OVER (PARTITION BY date_ ORDER BY CASE WHEN {{order_by_value}} = 1 THEN total_value ELSE orders_count END DESC) AS rank_
FROM stocks_data 
) s
WHERE s.rank_ <= 10
