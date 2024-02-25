WITH google_calendar_meetings AS ( -- selecting all meetings attended by Support agents
  SELECT a.login
    , CASE WHEN (c.summary ILIKE 'pr-%' OR c.summary ILIKE 'pr -%') THEN 'PR'
        WHEN (c.summary ILIKE 'poc-%' OR c.summary ILIKE 'poc -%') THEN 'POC'
        WHEN (c.summary ILIKE 'tr-%' OR c.summary ILIKE 'tr -%') THEN 'TR'
        WHEN (c.summary ILIKE 'bs-%' OR c.summary ILIKE 'bs -%') THEN 'BS'
        WHEN (c.summary ILIKE '121-%' OR c.summary ILIKE '121 -%') THEN '121'
        WHEN (c.summary ILIKE 'mt-%' OR c.summary ILIKE 'mt -%') THEN 'MT'
        WHEN (c.summary ILIKE 'o-%' OR c.summary ILIKE 'o -%') THEN 'O'
        WHEN c.summary ILIKE '%all hand%' THEN 'AH' END AS meeting_code
    , c.summary
    , c.start_at
    , c.end_at
    FROM google_calendar c
    JOIN agents_data a ON c.email = a.email AND c.end_at < a.end_date + INTERVAL '1' DAY
    WHERE TRUE
    AND (c.summary ILIKE 'pr-%'             -- projects
         OR c.summary ILIKE 'pr -%'
         OR c.summary ILIKE 'poc-%'         -- POC duties
         OR c.summary ILIKE 'poc -%'
         OR c.summary ILIKE 'tr-%'          -- trainings
         OR c.summary ILIKE 'tr -%'
         OR c.summary ILIKE 'bs-%'          -- buddy support
         OR c.summary ILIKE 'bs -%'
         OR c.summary ILIKE '121-%'         -- 121 meetings
         OR c.summary ILIKE '121 -%'
         OR c.summary ILIKE 'mt-%'          -- meetings inlcuding team meetings, support all hands etc.
         OR c.summary ILIKE 'mt -%'
         OR c.summary ILIKE 'o-%'           -- other, ex.HR related
         OR c.summary ILIKE 'o -%'
         OR c.summary ILIKE '%all hand%'    -- company wide all hands meetings
         )
    AND c.user_confirmation = 'accepted'
    AND c.event_status = 'confirmed'
)
, same_day_meetings AS ( -- calaculating meeting time (in minutes) for meetings that started and ended on the same day
  SELECT *
  , DATE_PART('hour', end_at - start_at) * 60
    + DATE_PART('minute', end_at - start_at) AS meeting_time
  FROM google_calendar_meetings
  WHERE TRUE
  AND start_at::date = end_at::date -- same day meetings
)
, not_same_day_meetings AS ( -- calaculating meeting time (in minutes) for meetings that started and ended on different days
  SELECT *
  , DATE_PART('hour', end_at::date - start_at) * 60
    + DATE_PART('minute', end_at::date - start_at) AS meeting_time_start_day
  , DATE_PART('hour', end_at - end_at::date) * 60
    + DATE_PART('minute', end_at - end_at::date) AS meeting_time_end_day
  FROM google_calendar_meetings
  WHERE TRUE
  AND start_at::date != end_at::date -- not same day meetings
)
, meetings_base AS ( -- joining all meetings together
  SELECT login
  , meeting_code
  , summary
  , start_at
  , end_at
  , meeting_time
  , FALSE AS was_split
  FROM same_day_meetings
  UNION ALL
  SELECT login
  , meeting_code
  , summary
  , start_at
  , end_at::date AS end_at
  , meeting_time_start_day AS meeting_time
  , TRUE AS was_split
  FROM not_same_day_meetings
  UNION ALL
  SELECT login
  , meeting_code
  , summary
  , end_at::date AS start_at
  , end_at
  , meeting_time_end_day AS meeting_time
  , TRUE AS was_split
  FROM not_same_day_meetings
)
-- next 3 CTEs used for solving issue with overlapping meetings which were accepted and not cancelled
, previous_meeting_end AS ( -- adding timestamp of meeting that took place directly before
    SELECT *
    , lag(end_at) OVER (PARTITION BY login ORDER BY start_at) AS prev_meet_end
    FROM meetings_base
)
, previous_meeting_max_end AS ( -- adding timestamp of previous meeting which ended last
    SELECT *
    , MAX(prev_meet_end) OVER (PARTITION BY login ORDER BY start_at) AS prev_meet_max_end
    FROM previous_meeting_end
)
, final_meeting_times AS ( -- calculating final meeting times in minutes
    SELECT *
    , COALESCE(CASE WHEN start_at >= prev_meet_max_end THEN meeting_time
        WHEN start_at < prev_meet_max_end AND end_at <= prev_meet_max_end THEN 0
        WHEN start_at < prev_meet_max_end AND end_at > prev_meet_max_end THEN 
                (DATE_PART('hour', end_at - prev_meet_max_end) * 60
                + DATE_PART('minute', end_at - prev_meet_max_end))
        END, meeting_time) AS final_meeting_time
    FROM previous_meeting_max_end
)
SELECT start_at::date AS date_
, login
, meeting_code
, SUM(final_meeting_time) / 60 AS meetings_hours
FROM final_meeting_times
GROUP BY date_, login, meeting_code
HAVING SUM(final_meeting_time) > 0
