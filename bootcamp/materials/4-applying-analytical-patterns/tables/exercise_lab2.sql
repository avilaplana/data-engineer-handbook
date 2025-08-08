CREATE TABLE web_events_dashboard AS
WITH combined AS (
	SELECT
		COALESCE(d.browser_type, 'N/A') as browser_type,
		COALESCE(d.os_type, 'N/A') as os_type,
		e.*,
		CASE WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
			WHEN referrer LIKE '%eczachly%' THEN 'On Site'
			WHEN referrer LIKE '%dataengineer.io%' THEN 'On Site'
			WHEN referrer LIKE '%t.co%' THEN 'On Twitter'
			WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
			WHEN referrer LIKE '%instagram%' THEN 'Instagram'
			WHEN referrer IS NULL THEN 'Direct'
			ELSE 'Other'
		END	AS referrer_mapped
	FROM events e
	JOIN devices d
	on e.device_id = d.device_id
)

-- SELECT
-- 	referrer_mapped,
-- 	COUNT(1) AS number_of_site_hits,
-- 	COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS number_of_signup_visits,
-- 	COUNT(CASE WHEN url LIKE '/contact' THEN 1 END) AS number_of_contact_visits,
-- 	COUNT(CASE WHEN url LIKE '/login' THEN 1 END) AS number_of_login_visits
-- FROM combined
-- GROUP BY referrer_mapped


SELECT
	COALESCE(referrer_mapped, '(overall)') as referrer,
	COALESCE(browser_type, '(overall)') as browser_type,
	COALESCE(os_type, '(overall)') as os_type,
	COUNT(1) AS number_of_site_hits,
	COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS number_of_signup_visits,
	COUNT(CASE WHEN url LIKE '/contact' THEN 1 END) AS number_of_contact_visits,
	COUNT(CASE WHEN url LIKE '/login' THEN 1 END) AS number_of_login_visits,
	CAST(COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS REAL)/COUNT(1) AS PCT_visited_signup
FROM combined
GROUP BY GROUPING SETS (
	(referrer_mapped, browser_type, os_type),
	(os_type),
	(browser_type),
	(referrer_mapped),
	()
)
HAVING COUNT(1) > 100
ORDER BY CAST(COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS REAL)/COUNT(1) DESC