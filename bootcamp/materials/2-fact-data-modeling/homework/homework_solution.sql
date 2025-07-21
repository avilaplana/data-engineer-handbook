-- - A query to deduplicate `game_details` from Day 1 so there's no duplicates

WITH deduped AS (
	SELECT 
		g.game_date_est,
		g.season,
		g.home_team_id,
		gd.*,
		ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
	FROM game_details gd 
	JOIN games g
	on gd.game_id = g.game_id
)

SELECT *  		
FROM deduped
WHERE row_num = 1;

-- - A DDL for an `user_devices_cumulated` table that has:
--   - a `device_activity_datelist` which tracks a users active days by `browser_type`
--   - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--     - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

CREATE TABLE user_devices_cumulated (
	user_id TEXT,
	browser_type TEXT,
	-- list of the dates in the past where the user was active
	device_activity_datelist DATE[],
	-- the current date for the user
	date DATE,
	PRIMARY KEY(user_id, browser_type, date)
)

-- - A cumulative query to generate `device_activity_datelist` from `events`

INSERT INTO user_devices_cumulated
WITH events_with_browser_type AS (
	SELECT 
		e.*, d.browser_type
	FROM events e 
	JOIN devices d
	ON e.device_id = d.device_id
	WHERE user_id IS NOT NULL
), yesterday AS (
	SELECT * 
	FROM user_devices_cumulated
	WHERE date = Date('2023-01-30')
), today AS (
	SELECT 
		CAST(user_id AS TEXT) as user_id,
		DATE(CAST(event_time AS TIMESTAMP)) AS date_active,
		browser_type
	FROM events_with_browser_type
	WHERE 
		DATE(CAST(event_time AS TIMESTAMP)) = Date('2023-01-31')
		AND user_Id IS NOT NULL
	GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP)),browser_type
)	

SELECT 
	COALESCE(T.user_id, y.user_id) AS user_id,
	COALESCE(t.browser_type, y.browser_type) as browser_type,
	CASE WHEN y.device_activity_datelist is NULL
		THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL
		THEN y.device_activity_datelist
		ELSE ARRAY[t.date_active] || y.device_activity_datelist
		END AS device_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date	
FROM today t FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id AND t.browser_type = y.browser_type

-- - A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 
WITH users as (
	SELECT * 
	FROM user_devices_cumulated
	WHERE date = DATE('2023-01-31')
), series AS (
	SELECT 
		* 
	FROM generate_series(DATE('2023-01-02'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
), place_holder_ints AS (
		SELECT 
		CASE WHEN 
			device_activity_datelist @> ARRAY [DATE(series_date)]
		THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
		ELSE 0
		END AS placeholder_int_value,
		* 
	FROM users CROSS JOIN series 
)

SELECT 
	user_id,
	browser_type,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int,
	BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS mounthly_active
FROM place_holder_ints
GROUP BY user_id, browser_type

-- - A DDL for `hosts_cumulated` table 
--   - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity

CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY(host, date)	
)

-- - The incremental query to generate `host_activity_datelist`

INSERT INTO hosts_cumulated
WITH yesterday AS (
	SELECT * 
	FROM hosts_cumulated
	WHERE date = Date('2023-01-30')
), today AS (
	SELECT 
		host,
		DATE(CAST(event_time AS TIMESTAMP)) AS date_active	
	FROM events
	WHERE 
		DATE(CAST(event_time AS TIMESTAMP)) = Date('2023-01-31')		
	GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT 
	COALESCE(t.host, y.host) AS host,
	CASE WHEN y.host_activity_datelist is NULL
		THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL
		THEN y.host_activity_datelist
		ELSE ARRAY[t.date_active] || y.host_activity_datelist
		END AS host_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date	
FROM today t FULL OUTER JOIN yesterday y
ON t.host = y.host

-- - A monthly, reduced fact table DDL `host_activity_reduced`
--    - month
--    - host
--    - hit_array - think COUNT(1)
--    - unique_visitors array -  think COUNT(DISTINCT user_id)

CREATE TABLE host_activity_reduced(
	month DATE,
	host TEXT,
	hit_array REAL[],
	unique_visitors_array REAL[],
	PRIMARY KEY (host, month)
)

-- - An incremental query that loads `host_activity_reduced`
--   - day-by-day

INSERT INTO host_activity_reduced
WITH yesterday AS (
	SELECT 
		* 
	FROM host_activity_reduced
	WHERE month = DATE('2023-01-01')
), daily_aggregate AS (
	SELECT 
		host,
		DATE(event_time) AS date,
		COUNT(1) AS num_site_hits,
		COUNT(DISTINCT user_id) AS num_user_hits		
	FROM events
	WHERE DATE(event_time) = DATE('2023-01-31')
	GROUP BY host, DATE(event_time)
)
SELECT 
	COALESCE(ya.month, DATE_TRUNC('month',da.date)) AS month,
	COALESCE(da.host, ya.host) AS host,	
	CASE WHEN ya.hit_array IS NOT NULL 
			THEN ya.hit_array || ARRAY[COALESCE(da.num_site_hits,0)]
		WHEN ya.hit_array IS NULL
			THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_site_hits,0)]
	END AS hit_array,
		CASE WHEN ya.unique_visitors_array IS NOT NULL 
			THEN ya.unique_visitors_array || ARRAY[COALESCE(da.num_user_hits,0)]
		WHEN ya.unique_visitors_array IS NULL
			THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_user_hits,0)]
	END AS unique_visitors_array		
FROM daily_aggregate da
FULL OUTER JOIN yesterday ya ON	
da.host = ya.host
ON CONFLICT(host,month)
DO
	UPDATE SET hit_array = EXCLUDED.hit_array, unique_visitors_array = EXCLUDED.unique_visitors_array;
