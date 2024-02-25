WITH productivity_base AS ( -- importing productivity data from tickets and tasks
    SELECT bo.login
    , bo.date_
    , bo.type::varchar AS task_type
    , SUM(bo.productivity) AS productivity
    FROM tasks_data bo
    GROUP BY bo.login, bo.date_, bo.type
    UNION ALL
    SELECT i.login
    , i.date_
    , 'chat' AS task_type
    , SUM(i.productivity) AS productivity
    FROM tickets_data i
    GROUP BY i.login, i.date_, task_type
)
, tat_base AS ( -- importing scores for task types from tasks_scores table
    SELECT t.*
    FROM tasks_scores t
)
, productivity_tat AS ( -- calculating productivity score on date-task type level
    SELECT p.login
    , p.date_
    , p.task_type
    , p.productivity
    , t.tat
    , p.productivity * t.tat AS prod_score
    FROM productivity_base p
    LEFT JOIN tat_base t ON p.task_type = t.task_type
    JOIN agents_data a ON p.login = a.login
)
SELECT login
, date_
, SUM(prod_score) AS prod_score_day
FROM productivity_tat
GROUP BY login, date_
