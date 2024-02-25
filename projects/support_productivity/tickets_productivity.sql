-- Tickets productivity:
-- - inbound tickets = 1
-- - outbound tickets with customer response = 1
-- - outbound tickets with no customer response = 0.1
-- Total ticket productivity is divided by number of messages sent on ticket giving agent's productivity

WITH full_prod_tickets AS ( -- selecting tickets with full productivity = 1
    -- inbound tickets
    SELECT ticket_id
    , 1 AS ticket_prod
    FROM tickets
    WHERE TRUE
    AND source_delivered_as != 'automated'
    UNION
    -- outbound tickets with customer response
    SELECT DISTINCT t.ticket_id
    , 1 AS ticket_prod
    FROM tickets t
    JOIN messages m ON t.ticket_id = m.ticket_id AND m.author_id::varchar LIKE '6%'
    WHERE TRUE
    AND t.source_delivered_as = 'automated'
)
, partial_prod_tickets AS ( -- selecting tickets with partial productivity
    -- outbound tickets with no customer response
    SELECT ticket_id
    , 0.1 AS ticket_prod
    FROM tickets
    WHERE TRUE
    AND ticket_id NOT IN (SELECT ticket_id FROM full_prod_tickets)
)
, tickets_messages AS ( -- counting number of messages sent by agents on each ticket
    SELECT m.ticket_id
    , COUNT(m.message_id) AS all_messages
    , COALESCE(fp.ticket_prod, pp.ticket_prod) AS ticket_prod
    FROM messages m
    LEFT JOIN full_prod_tickets fp ON m.ticket_id = fp.ticket_id
    LEFT JOIN partial_prod_tickets pp ON m.ticket_id = pp.ticket_id
    WHERE TRUE
    AND m.type NOT IN ('note', 'note_and_reopen') -- actions not related to sending messages
    AND m.message_body IS NOT NULL
    AND m.author_id LIKE '5%' -- filtering only non-customer intercom users
    GROUP BY m.ticket_id, 3
)
SELECT im.message_created_at::date AS date_
, im.ticket_id
, im.author_id
, LEFT(ia.email, LENGTH(ia.email) - 10) AS login
-- , COUNT(im.message_id) AS agent_messages
-- , tm.all_messages
, (COUNT(im.message_id)::numeric / tm.all_messages) * tm.ticket_prod AS productivity
FROM messages im
JOIN tickets_messages tm ON im.ticket_id = tm.ticket_id
JOIN admins ia ON im.author_id::integer = ia.admin_id
WHERE TRUE
AND im.type NOT IN ('note', 'note_and_reopen') -- actions not related to sending messages
AND im.message_body IS NOT NULL
AND im.author_id::varchar LIKE '5%' -- filtering only non-customer intercom users
GROUP BY date_, im.ticket_id, im.author_id, ia.email, tm.all_messages, tm.ticket_prod
