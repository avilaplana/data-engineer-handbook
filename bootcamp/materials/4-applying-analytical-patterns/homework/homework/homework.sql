-- # Week 4 Applying Analytical Patterns
-- The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1
--
-- - A query that does state change tracking for `players`
--   - A player entering the league should be `New`
--   - A player leaving the league should be `Retired`
--   - A player staying in the league should be `Continued Playing`
--   - A player that comes out of retirement should be `Returned from Retirement`
--   - A player that stays out of the league should be `Stayed Retired`


 CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );
 CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');

 CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
 	 state_track TEXT,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );


WITH last_season AS (
    SELECT * FROM players
    WHERE current_season = 2001

), this_season AS (
     SELECT * FROM player_seasons
    WHERE season = 2002
)
INSERT INTO players
SELECT
        COALESCE(ls.player_name, ts.player_name) as player_name,
        COALESCE(ls.height, ts.height) as height,
        COALESCE(ls.college, ts.college) as college,
        COALESCE(ls.country, ts.country) as country,
        COALESCE(ls.draft_year, ts.draft_year) as draft_year,
        COALESCE(ls.draft_round, ts.draft_round) as draft_round,
        COALESCE(ls.draft_number, ts.draft_number)
            as draft_number,
        COALESCE(ls.seasons,
            ARRAY[]::season_stats[]
            ) || CASE WHEN ts.season IS NOT NULL THEN
                ARRAY[ROW(
                ts.season,
                ts.pts,
                ts.ast,
                ts.reb, ts.weight)::season_stats]
                ELSE ARRAY[]::season_stats[] END
            as seasons,
         CASE
             WHEN ts.season IS NOT NULL THEN
                 (CASE WHEN ts.pts > 20 THEN 'star'
                    WHEN ts.pts > 15 THEN 'good'
                    WHEN ts.pts > 10 THEN 'average'
                    ELSE 'bad' END)::scoring_class
             ELSE ls.scoring_class
         END as scoring_class,
		 CASE
		 	WHEN ts.season IS NOT NULL THEN 0
		 	ELSE ls.years_since_last_active + 1
		  END AS years_since_last_active, -- REVIEW THIS
         ts.season IS NOT NULL as is_active,
		 CASE
		 	WHEN ts.season IS NOT NULL AND ls.current_season IS NULL THEN 'New'
			WHEN ts.season IS NOT NULL AND ls.is_active is True THEN 'Continued Playing'
			WHEN ts.season IS NULL AND ls.is_active  is True THEN 'Retired'
			WHEN ts.season IS NOT NULL AND ls.is_active is False THEN 'Returned from Retirement'
			WHEN ts.season IS NULL AND ls.is_active is False THEN 'Stayed Retired'
		 END AS state_track,
         2002 AS current_season
    FROM last_season ls
    FULL OUTER JOIN this_season ts
    ON ls.player_name = ts.player_name

SELECT * FROM PLAYERS  WHERE player_name = 'Michael Jordan' ORDER BY current_season DESC
-- - A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   - Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team?

WITH combined AS (
	SELECT
		g.season,
		g.game_id,
		gd.player_name,
		t.nickname AS team_name,
		gd.pts
	FROM game_details gd
	JOIN games g
	ON gd.game_id = g.game_id
	JOIN teams t
	ON gd.team_id = t.team_id
	WHERE gd.pts IS NOT NULL
), deduped AS (
	SELECT
		*,
		row_number() OVER (PARTITION BY season, game_id, player_name, team_name, pts) AS row_num
	FROM combined
), grouped AS (
	SELECT
		COALESCE(CAST(season AS TEXT),'(overall)') AS season,
		COALESCE(player_name, '(overall)') AS player_name,
	    COALESCE(team_name, '(overall)') AS team_name,
		SUM(pts) AS points
	FROM  deduped
	WHERE row_num = 1
	GROUP BY GROUPING SETS (
		(player_name, team_name),
		(player_name, season),
		(team_name)
	)
) , player_team_points AS (
	SELECT
		player_name,
		team_name,
		SUM(points) AS all_points
	FROM grouped
	WHERE player_name != '(overall)' AND team_name != '(overall)' AND season = '(overall)'
	GROUP BY player_name, team_name
), most_points_team AS (
	SELECT
		*,
		row_number() OVER (PARTITION BY team_name ORDER BY all_points DESC) AS row_num
	FROM player_team_points
)

SELECT
	player_name,
	team_name,
	all_points
FROM most_points_team
WHERE row_num = 1

--     - player and season
--       - Answer questions like who scored the most points in one season?

WITH combined AS (
	SELECT
		g.season,
		g.game_id,
		gd.player_name,
		t.nickname AS team_name,
		gd.pts
	FROM game_details gd
	JOIN games g
	ON gd.game_id = g.game_id
	JOIN teams t
	ON gd.team_id = t.team_id
	WHERE gd.pts IS NOT NULL
), deduped AS (
	SELECT
		*,
		row_number() OVER (PARTITION BY season, game_id, player_name, team_name, pts) AS row_num
	FROM combined
), grouped AS (
	SELECT
		COALESCE(CAST(season AS TEXT),'(overall)') AS season,
		COALESCE(player_name, '(overall)') AS player_name,
	    COALESCE(team_name, '(overall)') AS team_name,
		SUM(pts) AS points
	FROM  deduped
	WHERE row_num = 1
	GROUP BY GROUPING SETS (
		(player_name, team_name),
		(player_name, season),
		(team_name)
	)
), players_points_season AS (
	SELECT
		season,
		player_name,
		points,
		row_number() OVER (PARTITION BY season ORDER BY points DESC) AS row_num
		FROM grouped
	WHERE season != '(overall)' AND player_name != '(overall)' AND team_name = '(overall)'
)

SELECT
	season,
	player_name,
	points
FROM players_points_season
WHERE row_num = 1

--     - team
--       - Answer questions like which team has won the most games?

WITH combined AS (
	SELECT
		g.season,
		g.game_id,
		gd.player_name,
		t.nickname AS team_name,
		gd.pts,
		CASE
	        WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN 1
	        WHEN gd.team_id != g.home_team_id AND home_team_wins = 0 THEN 1
	        ELSE 0
    	END AS win
	FROM game_details gd
	JOIN games g
	ON gd.game_id = g.game_id
	JOIN teams t
	ON gd.team_id = t.team_id
	WHERE gd.pts IS NOT NULL
), deduped AS (
	SELECT
		*,
		row_number() OVER (PARTITION BY season, game_id, player_name, team_name, pts) AS row_num
	FROM combined
), grouped AS (
	SELECT
		COALESCE(CAST(season AS TEXT),'(overall)') AS season,
		COALESCE(player_name, '(overall)') AS player_name,
	    COALESCE(team_name, '(overall)') AS team_name,
		SUM(win) AS win,
		SUM(pts) AS points
	FROM  deduped
	WHERE row_num = 1
	GROUP BY GROUPING SETS (
		(player_name, team_name),
		(player_name, season),
		(team_name)
	)
), team_win AS (
	SELECT
		team_name,
		win,
		row_number() OVER (ORDER BY win DESC) AS row_num
		FROM grouped
		WHERE season = '(overall)' AND player_name = '(overall)' AND team_name != '(overall)'
)
SELECT
	team_name,
	win
FROM team_win
WHERE row_num = 1

-- - A query that uses window functions on `game_details` to find out the following things:
--   - What is the most games a team has won in a 90 game stretch?


WITH game_teams AS (
	SELECT
		game_id,
		team_id
	FROM game_details
	GROUP BY game_id, team_id
), combined AS (
	SELECT
			g.season,
			g.game_date_est,
			g.game_id,
			t.nickname AS team_name,
			CASE
		        WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN 1
		        WHEN gd.team_id != g.home_team_id AND home_team_wins = 0 THEN 1
		        ELSE 0
	    	END AS win
	FROM game_teams gd
	JOIN games g
	ON gd.game_id = g.game_id
	JOIN teams t
	ON gd.team_id = t.team_id
), deduped AS (
	SELECT
		*,
		row_number() OVER (PARTITION BY season, game_id, team_name, win) AS row_num
	FROM combined
), filtered AS (
	SELECT
		season,
		game_date_est,
		game_id,
		team_name,
		win
	FROM deduped
	WHERE row_num = 1
), ninty_games_win  AS (
	SELECT
	*,
	SUM(win) OVER (PARTITION BY team_name ORDER BY game_date_est ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS ninety_game_win_counter
	FROM filtered
)
SELECT
	team_name,
MAX(ninety_game_win_counter) AS max_win_ninety_matches
FROM ninty_games_win
GROUP BY team_name
ORDER BY max_win_ninety_matches DESC

--   - How many games in a row did LeBron James score over 10 points a game?
WITH combined AS (
	SELECT
	g.game_date_est,
	gd.game_id,
	gd.player_name,
	gd.pts,
	CASE
		WHEN gd.pts > 10 THEN 1
		ELSE 0
	END AS over_ten_points
	FROM game_details gd
	JOIN games g
	ON gd.game_id = g.game_id
	WHERE gd.pts IS NOT NULL
), lebron AS (
    SELECT
        game_date_est,
        game_id,
        pts,
        over_ten_points,
        ROW_NUMBER() OVER (ORDER BY game_date_est) AS rn
    FROM combined
    WHERE player_name = 'LeBron James'
), streak_groups AS (
    SELECT *,
           rn - ROW_NUMBER() OVER (
               PARTITION BY over_ten_points
               ORDER BY game_date_est
           ) AS grp
    FROM lebron
    WHERE over_ten_points = 1
),

streak_counts AS (
    SELECT grp, COUNT(*) AS streak_count
    FROM streak_groups
    GROUP BY grp
)

SELECT MAX(streak_count) AS longest_streak
FROM streak_counts;

