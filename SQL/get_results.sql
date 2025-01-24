SELECT
	count(1) AS "Count of Values",
	avg(fc."CALCULATED_RATE") AS "Calculated Rate Average",
	sum(fc."CALCULATED_RATE") AS "Calculated Rate Sum",
	min(fc."CALCULATED_RATE") AS "Minumun Calculated Rate Value",
	max(fc."CALCULATED_RATE") AS "Maximun Calculated Rate Value"
FROM
	consumptions.fact_consumptions fc;