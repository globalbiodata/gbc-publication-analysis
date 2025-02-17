-- first, set resources to not be latest
UPDATE resource r JOIN prediction p ON r.prediction_id = p.id JOIN (
	SELECT r2.short_name, MAX(p2.date) as latest_date
    FROM resource r2 join prediction p2 on r2.prediction_id = p2.id
    GROUP BY short_name
) x ON x.short_name = r.short_name
SET r.is_latest = 0
WHERE x.latest_date != p.date;

-- then, set the resources with most recent prediction date to be latest
UPDATE resource r JOIN prediction p ON r.prediction_id = p.id JOIN (
	SELECT r2.short_name, MAX(p2.date) as latest_date
    FROM resource r2 join prediction p2 on r2.prediction_id = p2.id
    GROUP BY short_name
) x ON x.short_name = r.short_name
SET r.is_latest = 1
WHERE x.latest_date = p.date;