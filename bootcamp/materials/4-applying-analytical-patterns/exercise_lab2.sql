-- CREATE TABLE web_events_dashboard AS
-- WITH combined AS (
-- 	SELECT
-- 		COALESCE(d.browser_type, 'N/A') as browser_type,
-- 		COALESCE(d.os_type, 'N/A') as os_type,
-- 		e.*,
-- 		CASE WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
-- 			WHEN referrer LIKE '%eczachly%' THEN 'On Site'
-- 			WHEN referrer LIKE '%dataengineer.io%' THEN 'On Site'
-- 			WHEN referrer LIKE '%t.co%' THEN 'On Twitter'
-- 			WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
-- 			WHEN referrer LIKE '%instagram%' THEN 'Instagram'
-- 			WHEN referrer IS NULL THEN 'Direct'
-- 			ELSE 'Other'
-- 		END	AS referrer_mapped
-- 	FROM events e
-- 	JOIN devices d
-- 	on e.device_id = d.device_id
-- )

-- SELECT
-- 	referrer_mapped,
-- 	COUNT(1) AS number_of_site_hits,
-- 	COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS number_of_signup_visits,
-- 	COUNT(CASE WHEN url LIKE '/contact' THEN 1 END) AS number_of_contact_visits,
-- 	COUNT(CASE WHEN url LIKE '/login' THEN 1 END) AS number_of_login_visits
-- FROM combined
-- GROUP BY referrer_mapped


-- SELECT
-- 	COALESCE(referrer_mapped, '(overall)') as referrer,
-- 	COALESCE(browser_type, '(overall)') as browser_type,
-- 	COALESCE(os_type, '(overall)') as os_type,
-- 	COUNT(1) AS number_of_site_hits,
-- 	COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS number_of_signup_visits,
-- 	COUNT(CASE WHEN url LIKE '/contact' THEN 1 END) AS number_of_contact_visits,
-- 	COUNT(CASE WHEN url LIKE '/login' THEN 1 END) AS number_of_login_visits,
-- 	CAST(COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS REAL)/COUNT(1) AS PCT_visited_signup
-- FROM combined
-- GROUP BY GROUPING SETS (
-- 	(referrer_mapped, browser_type, os_type),
-- 	(os_type),
-- 	(browser_type),
-- 	(referrer_mapped),
-- 	()
-- )
-- HAVING COUNT(1) > 100
-- ORDER BY CAST(COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS REAL)/COUNT(1) DESC


-- SELECT
-- 	COALESCE(referrer_mapped, '(overall)') as referrer,
-- 	COALESCE(browser_type, '(overall)') as browser_type,
-- 	COALESCE(os_type, '(overall)') as os_type,
-- 	COUNT(1) AS number_of_site_hits,
-- 	COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS number_of_signup_visits,
-- 	COUNT(CASE WHEN url LIKE '/contact' THEN 1 END) AS number_of_contact_visits,
-- 	COUNT(CASE WHEN url LIKE '/login' THEN 1 END) AS number_of_login_visits,
-- 	CAST(COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS REAL)/COUNT(1) AS PCT_visited_signup
-- FROM combined
-- GROUP BY ROLLUP (referrer_mapped, browser_type, os_type)
-- HAVING COUNT(1) > 100
-- ORDER BY CAST(COUNT(CASE WHEN url LIKE '/signup' THEN 1 END) AS REAL)/COUNT(1) DESC

-- the following is the same as
-- GROUP BY ROLLUP (referrer_mapped, browser_type, os_type):

-- GROUP BY GROUPING SETS (
-- (referrer_mapped),
-- (referrer_mapped, browser_type),
-- (referrer_mapped, browser_type, os_type)
-- )

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
), aggregated AS (
select
	c1.user_id,
	c1.url AS to_url,
	c2.url AS from_url,
	MIN(c1.event_time::timestamp - c2.event_time::timestamp) as duration
FROM combined c1 JOIN combined c2
	ON c1.user_id = c2.user_id
	AND DATE(c1.event_time) = DATE(c2.event_time)
	AND c1.event_time > c2.event_time
GROUP BY c1.user_id, c1.url, c2.url
)

SELECT
	to_url,
	from_url,
	COUNT(1) AS number_users,
	MIN(duration) AS min_duration,
	MAX(duration) AS max_duration,
	AVG(duration) AS avg_duration
FROM aggregated
group by to_url, from_url
HAVING COUNT(1)> 100