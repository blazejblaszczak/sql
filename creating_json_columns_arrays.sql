WITH alerts_base AS ( -- selecting all price alerts from chosen time frame
    SELECT pa.user_id
    , i.symbol
    , pa.price * 0.01 AS price
    , pa.type
    , pa.status
    FROM price_alerts pa
    JOIN users u ON pa.user_id = u.id
    JOIN instruments i ON pa.instrument_id = i.id
    ORDER BY pa.user_id, i.symbol
)
, alerts AS ( -- aggregating all alerts on user level
    SELECT user_id, COUNT(price_alert) AS alerts_count, ARRAY_AGG(price_alert) AS price_alerts
    FROM (
        SELECT user_id, ((SELECT d FROM (SELECT symbol, price, type, status) d)) AS price_alert
        FROM alerts_base
    ) a
    GROUP BY user_id
)
, users_symbols AS ( -- selecting unique user-symbol pairs
    SELECT DISTINCT user_id, symbol
    FROM alerts_base
)
, trades_base AS ( -- selecting all trades related to price alerts
    SELECT o.user_id
    , o.side
    , o.symbol
    , ROUND(o.filled_qty * o.filled_avg_price * 0.01, 2) AS value_usd
    FROM al_orders o
    JOIN users_symbols u ON o.user_id = u.user_id AND o.symbol = u.symbol -- filtering only price alert related symbols
    WHERE TRUE
    AND status = 'filled'
    ORDER BY o.user_id, o.side, o.symbol
)
, buy_trades AS ( -- aggregating all buy trades on user level
    SELECT user_id, COUNT(trade) AS buy_trades_count, ARRAY_AGG(trade) AS buy_trades
    FROM (
        SELECT user_id, ((SELECT d FROM (SELECT symbol, value_usd) d)) AS trade
        FROM trades_base
        WHERE side = 'buy'
    ) t
    GROUP BY user_id
)
, sell_trades AS ( -- aggregating all sell trades on user level
    SELECT user_id, COUNT(trade) AS sell_trades_count, ARRAY_AGG(trade) AS sell_trades
    FROM (
        SELECT user_id, ((SELECT d FROM (SELECT symbol, value_usd) d)) AS trade
        FROM trades_base
        WHERE side = 'sell'
    ) t
    GROUP BY user_id
)
, total_trades AS ( -- calculating total value of all/buy/sell trades for each user
    SELECT u.user_id
    , COALESCE(SUM(t.value_usd), 0) AS total_invested_usd
    , COALESCE(SUM(CASE WHEN t.side = 'buy' THEN value_usd END), 0) AS total_buy_usd
    , COALESCE(SUM(CASE WHEN t.side = 'sell' THEN value_usd END), 0) AS total_sell_usd
    FROM (SELECT DISTINCT user_id FROM alerts) u
    LEFT JOIN trades_base t ON u.user_id = t.user_id
    GROUP BY u.user_id
)
SELECT tt.user_id
, tt.total_invested_usd
, tt.total_buy_usd
, tt.total_sell_usd
, a.alerts_count
, a.price_alerts
, bt.buy_trades_count
, bt.buy_trades
, st.sell_trades_count
, st.sell_trades
FROM total_trades tt
JOIN alerts a ON tt.user_id = a.user_id
LEFT JOIN buy_trades bt ON tt.user_id = bt.user_id
LEFT JOIN sell_trades st ON tt.user_id = st.user_id
ORDER BY total_invested_usd DESC
