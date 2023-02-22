/*
Write a statement to create a table containing print providers with average
production time, reprint percent, last order timestamp, and primary shipping carrier
*/


CREATE TABLE print_providers AS
	WITH print_providers AS ( -- selecting all providers and calculating their reprint rate percentage
		SELECT print_provider_id
		, SUM(CASE WHEN reprint_flag IS TRUE THEN quantity END) AS reprinted_items
		, SUM(quantity) AS all_items
		, ROUND(((COALESCE(SUM(CASE WHEN reprint_flag IS TRUE THEN quantity END), 0)::numeric
			/ SUM(quantity)) * 100), 2) AS reprint_percent
		FROM items
		WHERE TRUE
		AND print_provider_id != 0 -- in ETL NULLs were changed to 0
		GROUP BY print_provider_id
	)
	, production_times AS ( -- calculating production time for each order
		SELECT DISTINCT i.print_provider_id
		, o.order_id
		, o.order_dt
		, o.fulfilled_dt
		,(DATE_PART('day', o.fulfilled_dt - o.order_dt) * 24 + 
			DATE_PART('hour', o.fulfilled_dt - o.order_dt)) * 60 +
			DATE_PART('minute', o.fulfilled_dt - o.order_dt) AS prod_time_minutes
		FROM orders o
		JOIN items i ON o.order_id = i.order_id
		WHERE TRUE
		AND o.fulfilled_dt IS NOT NULL
	)
	, avg_production_times AS ( -- calculating average production time in hours for each print provider
		SELECT print_provider_id
		, ROUND(AVG(prod_time_minutes)::numeric / 60, 2) AS avg_prod_time_hrs
		FROM production_times
		GROUP BY print_provider_id
	)
	, primary_carriers AS ( -- selecting primary carrier for each print provider, based on most number of orders for which particular shipment carrier was used
		SELECT DISTINCT ON (i.print_provider_id)
		i.print_provider_id
		, o.shipment_carrier AS primary_carrier
		, COUNT(DISTINCT o.order_id) AS carrier_used
		FROM orders o
		JOIN items i ON o.order_id = i.order_id
		WHERE TRUE
		AND o.shipment_carrier IS NOT NULL
		GROUP BY i.print_provider_id, o.shipment_carrier
		ORDER BY i.print_provider_id, carrier_used DESC
	)
	SELECT pp.print_provider_id
	, pt.avg_prod_time_hrs
	, pp.reprint_percent
	, pc.primary_carrier
	FROM print_providers pp
	LEFT JOIN avg_production_times pt ON pp.print_provider_id = pt.print_provider_id
	LEFT JOIN primary_carriers pc ON pp.print_provider_id = pc.print_provider_id
