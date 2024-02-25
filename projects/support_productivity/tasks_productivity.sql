-- Productivity is assigned only to agent who resolved/rejected task on day of resolution/rejection.
-- Only for UAR, productivity calculation is different. 1 productivity point is assigned
-- for every creation/resolution/rejection of UAR task or assignment to compliance team

WITH uar_tasks AS ( -- selecting UAR tasks
    SELECT id AS task_id
    FROM backend.backoffice_tasks
    WHERE TRUE
    AND type::text = 'uar'
)
, tasks_productivity AS ( -- adding productivity for resolved/rejected backoffice tasks
    SELECT replace(u.email, '@email.io', '') AS login
    , bt.resolved_at::date AS date_
    , bt.id AS task_id
    , bt.type::text AS type
    , 1 AS productivity
    FROM tasks bt
    JOIN gusers u ON bt.resolver_id = u.id
    WHERE TRUE
    AND state IN ('RESOLVED', 'REJECTED')
    AND type::text != 'uar'
)
, uar_productivity AS ( -- adding productivity for UAR tasks
    SELECT replace(l.author_email, '@shares.io', '') AS login
    , l.created::date AS date_
    , l.task_id
    , 'uar' AS type
    , 1 AS productivity
    FROM logs l
    JOIN uar_tasks u ON l.task_id = u.task_id
    WHERE TRUE
    AND (action IN ('create', 'resolve', 'reject')
            OR (action = 'assign' AND extra_info ILIKE 'bo-compliance%'))
)
, awaiting_customer_productivity AS ( -- adding productivity for "awaiting customer" tasks states
    SELECT replace(l.author_email, '@email.io', '') AS login
    , l.created::date AS date_
    , l.task_id
    , bt.type::text AS type
    , 1 AS productivity
    FROM logs l
    JOIN tasks bt ON l.task_id = bt.id
    LEFT JOIN tasks_productivity tp ON replace(l.author_email, '@shares.io', '') = tp.login AND l.task_id = tp.task_id
    WHERE TRUE
    AND lower(l.extra_info) ILIKE '%awaiting%'
    AND l.author_email != 'system'
    AND tp.login IS NULL
)
SELECT *
FROM tasks_productivity
UNION ALL
SELECT *
FROM uar_productivity
UNION ALL
SELECT *
FROM awaiting_customer_productivity
