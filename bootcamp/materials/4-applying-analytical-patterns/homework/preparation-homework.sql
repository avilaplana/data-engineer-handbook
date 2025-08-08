-- SELECT MIN(SEASON) AS first_season, MAX(SEASON) AS last_season from player_seasons

 -- CREATE TYPE season_stats AS (
 --                         season Integer,
 --                         pts REAL,
 --                         ast REAL,
 --                         reb REAL,
 --                         weight INTEGER
 --                       );
 -- CREATE TYPE scoring_class AS
 --     ENUM ('bad', 'average', 'good', 'star');


 -- CREATE TABLE players (
 --     player_name TEXT,
 --     height TEXT,
 --     college TEXT,
 --     country TEXT,
 --     draft_year TEXT,
 --     draft_round TEXT,
 --     draft_number TEXT,
 --     seasons season_stats[],
 --     scoring_class scoring_class,
 --     years_since_last_active INTEGER,
 --     is_active BOOLEAN,
 --     current_season INTEGER,
 --     PRIMARY KEY (player_name, current_season)
 -- );

-- DELETE FROM players
-- player_seasons min season is 1996

-- WITH last_season AS (
--     SELECT * FROM players
--     WHERE current_season = 2008

-- ), this_season AS (
--      SELECT * FROM player_seasons
--     WHERE season = 2009
-- )
-- INSERT INTO players
-- SELECT
--         COALESCE(ls.player_name, ts.player_name) as player_name,
--         COALESCE(ls.height, ts.height) as height,
--         COALESCE(ls.college, ts.college) as college,
--         COALESCE(ls.country, ts.country) as country,
--         COALESCE(ls.draft_year, ts.draft_year) as draft_year,
--         COALESCE(ls.draft_round, ts.draft_round) as draft_round,
--         COALESCE(ls.draft_number, ts.draft_number)
--             as draft_number,
--         COALESCE(ls.seasons,
--             ARRAY[]::season_stats[]
--             ) || CASE WHEN ts.season IS NOT NULL THEN
--                 ARRAY[ROW(
--                 ts.season,
--                 ts.pts,
--                 ts.ast,
--                 ts.reb, ts.weight)::season_stats]
--                 ELSE ARRAY[]::season_stats[] END
--             as seasons,
--          CASE
--              WHEN ts.season IS NOT NULL THEN
--                  (CASE WHEN ts.pts > 20 THEN 'star'
--                     WHEN ts.pts > 15 THEN 'good'
--                     WHEN ts.pts > 10 THEN 'average'
--                     ELSE 'bad' END)::scoring_class
--              ELSE ls.scoring_class
--          END as scoring_class,
-- 		 CASE
-- 		 	WHEN ts.season IS NOT NULL THEN 0
-- 		 	ELSE ls.years_since_last_active + 1
-- 		  END AS years_since_last_active, -- REVIEW THIS
--          ts.season IS NOT NULL as is_active,
--          2009 AS current_season
--     FROM last_season ls
--     FULL OUTER JOIN this_season ts
--     ON ls.player_name = ts.player_name


-- SELECT * FROM players ORDER BY current_season DESC

-- create table players_scd_table
-- (
-- 	player_name text,
-- 	scoring_class scoring_class,
-- 	is_active boolean,
-- 	start_season integer,
-- 	end_date integer,
-- 	current_season integer
-- );

insert into players_scd_table
WITH streak_started AS (
    SELECT player_name,
           current_season,
           scoring_class,
		   is_active,
           LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> scoring_class
               OR LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL
               AS did_change
    FROM players
    WHERE current_season <= 2009
),
     streak_identified AS (
         SELECT
            player_name,
                scoring_class,
				is_active,
                current_season,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            player_name,
            scoring_class,
			is_active,
            streak_identifier,
            MIN(current_season) AS start_date,
            MAX(current_season) AS end_date,
            2009 AS current_season
         FROM streak_identified
         GROUP BY 1,2,3,4
     )

     SELECT player_name, scoring_class, is_active, start_date, end_date,current_season
     FROM aggregated order by end_date desc