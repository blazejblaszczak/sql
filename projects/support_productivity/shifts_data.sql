WITH shift_hours AS ( -- calculating shift time (in hours) for each day
    SELECT s1.date
    , s1.agent_email
    , s1.shift_type
    , CASE WHEN UPPER(s1.shift_type) IN ('M', '1', '2', '3', 'WM', 'W1', 'W2', 'NS',
                                        'FC1', 'FC1.5', 'FC2', 'FC2.5', 'FC3', 'FCW1', 'FCW3', '3C', 'W1C',
                                        'CS1', 'CS2', 'CS3', 'PL1', 'PL2', 'TS1', 'TS2', 'IN') THEN 7.5
           WHEN UPPER(s1.shift_type) IN ('A1', 'A2') THEN 4.5
           WHEN UPPER(s1.shift_type) IN ('N', 'WN') THEN 3.5
           WHEN UPPER(s1.shift_type) IN ('FCN', 'FCWN') THEN 3.0
           ELSE 0 END
      + -- below part needs to be added if agent worked overnight, 
        -- part of night shift is assigned to shift day and second part to next day
      CASE WHEN UPPER(s2.shift_type) IN ('N', 'WN') THEN 4.0
           WHEN UPPER(s2.shift_type) IN ('FCN', 'FCWN') THEN 4.5
           ELSE 0 END
           AS shift_hours
    FROM support_agent_shifts s1
    -- below join required for night shifts working hours calculations
    LEFT JOIN support_agent_shifts s2 ON s1.agent_email = s2.agent_email
                                      AND s1.date = s2.date + INTERVAL '1' DAY
)
, final_output AS (
    SELECT s.date
    , a.login
    , s.shift_type
    , s.shift_hours
    FROM shift_hours s
    JOIN agents_data a ON s.agent_email = a.email AND s.date <= a.end_date
)
SELECT *
FROM final_output
