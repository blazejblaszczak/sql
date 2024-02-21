WITH portfolio_bins AS ( -- creating required portfolio bins
    SELECT * FROM
    (VALUES ('0-50', 0, 5000, 1)
            , ('50-100', 5001, 10000, 2)
            , ('100-200', 10001, 20000, 3)
            , ('200-500', 20001, 50000, 4)
            , ('500+', 50001, POWER(10, 10), 5)
            ) AS t (portfolio_bin, lower_bound, upper_bound, bin_position)
)
, users_data AS ( -- assigining users to bins
    SELECT ti.user_id
    , pb.portfolio_bin
    , pb.bin_position
    FROM total_invested ti
    JOIN portfolio_bins pb ON ti.portfolio_value BETWEEN pb.lower_bound AND pb.upper_bound
    WHERE TRUE
    AND ti.date = CURRENT_DATE - 1
    AND ti.symbols_count > 0
)
SELECT portfolio_bin
, COUNT(user_id) AS users_count
, COUNT(user_id)::numeric / (SELECT COUNT(user_id) FROM users_data) AS perc_of_all
FROM users_data
GROUP BY portfolio_bin, bin_position
ORDER BY bin_position
