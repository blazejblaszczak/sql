WITH user_base AS ( -- selecting users taking part in AB_ATT_PROMPT_PLACEMENT experiment
    SELECT user_id
    , MIN(event_at)::date AS experiment_start
    , feature_flag_value::numeric AS experiment_group
    FROM feature_flags
    WHERE TRUE
    AND feature_flag_value::numeric != 0 
    AND feature_flag_type ='AB_ATT_PROMPT_PLACEMENT'
    AND user_id IS NOT NULL
    GROUP BY user_id, experiment_group
)
, dates AS ( -- generating dates series
    SELECT generate_series((SELECT MIN(experiment_start) FROM user_base), current_date, INTERVAL '1' DAY) as date_
)
, users_dates AS ( -- creating user-date pairs for each day when user was in experiment
    SELECT d.date_
    , u.user_id
    , CASE WHEN u.experiment_group = 1 THEN 'Control' ELSE 'Test' END AS experiment_group
    FROM dates d
    CROSS JOIN user_base u
    WHERE TRUE
    AND d.date_ >= u.experiment_start::date
)
, user_permissions AS ( -- selecting all permissions given by users taking part in experiment
    SELECT u.user_id
    , CASE WHEN u.experiment_group = 1 THEN 'Control' ELSE 'Test' END AS experiment_group
    , u.experiment_start
    , pg.event_at::date AS permission_date
    , CASE WHEN ad_tracking_enabled = 'authorized' THEN 1 ELSE 0 END AS ad_tracking
    , CASE WHEN contacts_permissions = 'granted' THEN 1 ELSE 0 END AS contacts_permission
    , CASE WHEN notifications_permissions = 'granted' THEN 1 ELSE 0 END AS notifications_permission
    FROM user_base u
    LEFT JOIN user_permissions_granted pg ON u.user_id = pg.user_id
)
, experiment_start_status AS ( -- checking what was the permissions state for each user on the first day of experiment
    SELECT u.user_id
    , u.experiment_start
    , CASE WHEN u.experiment_group = 1 THEN 'Control' ELSE 'Test' END AS experiment_group
    , COALESCE(p.ad_tracking, 0) AS ad_tracking
    , COALESCE(p.contacts_permission, 0) AS contacts_permission
    , COALESCE(p.notifications_permission, 0) AS notifications_permission
    FROM user_base u
    LEFT JOIN user_permissions p ON u.user_id = p.user_id AND u.experiment_start = p.permission_date
)
, daily_status AS ( -- assigning permission changes to user-date pairs
    SELECT ud.date_
    , ud.user_id
    , ud.experiment_group
    , COALESCE(p.ad_tracking, s.ad_tracking) AS ad_tracking
    , COALESCE(p.contacts_permission, s.contacts_permission) AS contacts_permission
    , COALESCE(p.notifications_permission, s.notifications_permission) AS notifications_permission
    FROM users_dates ud
    LEFT JOIN user_permissions p ON ud.user_id = p.user_id AND ud.date_ = p.permission_date 
                                                           AND ud.experiment_group = p.experiment_group
                                                           AND ud.date_ != p.experiment_start
    LEFT JOIN experiment_start_status s ON ud.user_id = s.user_id AND ud.date_ = s.experiment_start
                                                                  AND ud.experiment_group = s.experiment_group
)
, data_partitions AS ( -- partitioning data to fill NULL values for permissions with last non NULL value
    SELECT *
    , SUM(CASE WHEN ad_tracking IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY user_id ORDER BY date_) AS permission_partition
    FROM daily_status
)
SELECT date_
, user_id
, experiment_group
, FIRST_VALUE(ad_tracking) OVER (PARTITION BY user_id, permission_partition ORDER BY date_) AS ad_tracking
, FIRST_VALUE(contacts_permission) OVER (PARTITION BY user_id, permission_partition ORDER BY date_) AS contacts_permission
, FIRST_VALUE(notifications_permission) OVER (PARTITION BY user_id, permission_partition ORDER BY date_) AS notifications_permission
FROM data_partitions
