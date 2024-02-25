WITH dates AS ( -- creating set of chosen period dates in chosen timeframe
    SELECT DISTINCT DATE_TRUNC({{period}}, date_) AS date_
    FROM (SELECT generate_series({{start_date}}, {{end_date}}, '1 day'::interval) AS date_) d
)
, shifts_base AS ( -- importing agent shifts data
    SELECT s.date
    , s.login
    , s.shift_type
    , s.shift_hours
    FROM shifts_data s
    WHERE TRUE
    AND s.date BETWEEN {{start_date}} AND {{end_date}}
)
, meetings_base AS ( -- importing agent meetings data
    SELECT m.date_
    , m.login
    , SUM(m.meetings_hours) AS meetings_hours
    FROM meetings_data m
    WHERE TRUE
    AND m.date_ BETWEEN {{start_date}} AND {{end_date}}
    GROUP BY m.date_, m.login
)
, work_hours_daily AS ( -- calcualting how many hours agent spent on work on particular day
  SELECT sb.date AS date_
  , sb.login
  , sb.shift_hours
  , COALESCE(mb.meetings_hours, 0) AS meetings_hours
  -- subtracting meetings time from shift hours, max cap of meeting hours daily = 7.5
  , sb.shift_hours - 
    COALESCE(CASE WHEN sb.shift_hours > 0 THEN (CASE WHEN mb.meetings_hours > 7.5 THEN 7.5 ELSE mb.meetings_hours END) ELSE 0 END, 0) AS work_hours
  FROM shifts_base sb
  LEFT JOIN meetings_base mb ON sb.date = mb.date_ AND sb.login = mb.login
)
, productivity_base AS ( -- importing productivity score data
    SELECT p.date_
    , p.login
    , p.prod_score_day
    FROM productivity_data p
    WHERE TRUE
    AND p.date_ BETWEEN {{start_date}} AND {{end_date}}
)
, daily_data AS ( -- joining work time with productivity
    SELECT COALESCE(w.date_, p.date_) AS date_
    , COALESCE(w.login, p.login) AS login
    , COALESCE(w.work_hours, 0) AS work_hours
    , COALESCE(p.prod_score_day, 0) AS prod_score_day
    FROM work_hours_daily w
    FULL JOIN productivity_base p ON w.date_ = p.date_ AND w.login = p.login
    JOIN agents_data a ON w.login = a.login
    WHERE TRUE
    AND (w.work_hours != 0 OR p.prod_score_day > 0)
)
, final_output AS ( -- final results for productivity score calculation
    SELECT DATE_TRUNC({{period}}, date_) AS date_
    , login
    , (SUM(prod_score_day) / NULLIF(SUM(work_hours), 0)) / 60 AS target_perc
    , SUM(prod_score_day) AS productivity_score
    , SUM(work_hours) AS hours_worked
    , SUM(prod_score_day) / NULLIF(SUM(work_hours), 0) AS productivity_hourly
    FROM daily_data
    GROUP BY 1, login
)
, agents_dates AS ( -- cross joining dates with agent logins
    SELECT d.date_
    , a.login
    FROM dates d
    CROSS JOIN (SELECT DISTINCT login FROM final_output) a
)
SELECT ad.date_
, ad.login
, fo.target_perc
, fo.productivity_score
, fo.hours_worked
, fo.productivity_hourly
FROM agents_dates ad
LEFT JOIN final_output fo ON ad.date_ = fo.date_ AND ad.login = fo.login
ORDER BY ad.login, ad.date_
