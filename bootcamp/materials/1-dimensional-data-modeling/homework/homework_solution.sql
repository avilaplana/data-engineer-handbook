-- DDL for `actors` table:

CREATE TYPE film_stats AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
)

CREATE TYPE quality_enum AS ENUM (
	'star', 'good', 'average', 'bad'
)

CREATE TABLE actors (
	actorid TEXT,
	actor TEXT,
	films film_stats[],
	quality_class quality_enum,
	is_active BOOLEAN,	
	year INTEGER,
	PRIMARY KEY (actorid, year)	
)


-- Cumulative table generation query
WITH last_year AS (
	SELECT * FROM actors
		WHERE year = 1969
),
	this_year AS (
		SELECT 
			*,
			AVG(rating) OVER (PARTITION BY actorid, year) as average_rating_this_year			
		FROM actor_films
		WHERE year = 1970
	),
	actor_film_year AS (
		SELECT 
			COALESCE(ly.actorid, ty.actorid) AS actorid,
			COALESCE(ly.actor, ty.actor) AS actor,
			COALESCE(ly.films,
				ARRAY[]::film_stats[]
				) || CASE WHEN ty.year IS NOT NULL THEN
					ARRAY[ROW(
						ty.film,
						ty.votes,
						ty.rating,
						ty.filmid)::film_stats]
						ELSE ARRAY[]::film_stats[] END
				AS films,
				CASE 
				WHEN ty.average_rating_this_year > 8 THEN 'star'::quality_enum
				WHEN ty.average_rating_this_year > 7 AND ty.average_rating_this_year <=8 THEN 'good'::quality_enum
				WHEN ty.average_rating_this_year > 6 AND ty.average_rating_this_year <=7 THEN 'average'::quality_enum
				WHEN ty.average_rating_this_year <= 6 THEN 'bad'::quality_enum
			END	AS quality_class,
			ty.year IS NOT NULL AS is_active,
			1970 AS year
		FROM last_year ly
		FULL OUTER JOIN this_year ty
		ON ly.actorid = ty.actorid
	)
SELECT 
	actorid,
	actor, 
	ARRAY_AGG(films) AS films,
	quality_class,
	is_active,
	year
FROM actor_film_year
GROUP BY actorid, actor, quality_class,is_active, year

-- Cumulative table generation query
INSERT INTO actors
WITH actors_with_film_stats AS (
	SELECT 
		actor,
		actorid,
		(film,
		votes,
		rating,
		filmid)::film_stats as film,
		rating,
		year
	FROM actor_films	
),
	actors_with_array_films AS (
		SELECT 
			actor,
			actorid,
			ARRAY_AGG(film) AS films,
			AVG(rating) as average_rating_this_year,
			year
		FROM actors_with_film_stats
		GROUP BY actor, actorid, year		
	)

	,last_year AS (
		SELECT * FROM actors
		WHERE year = 1973
),
	this_year AS (
		SELECT 
			*
		FROM actors_with_array_films
		WHERE year = 1974
	),
	actor_film_year AS (
		SELECT 
			COALESCE(ly.actorid, ty.actorid) AS actorid,
			COALESCE(ly.actor, ty.actor) AS actor,
			COALESCE(ly.films,
				ARRAY[]::film_stats[]
				) || CASE WHEN ty.year IS NOT NULL THEN
						ty.films
					ELSE ARRAY[]::film_stats[] END
				AS films,
				CASE 
    				WHEN ty.average_rating_this_year IS NOT NULL THEN
      					CASE 
        					WHEN ty.average_rating_this_year > 8 THEN 'star'::quality_enum
					        WHEN ty.average_rating_this_year > 7 AND ty.average_rating_this_year <= 8 THEN 'good'::quality_enum
					        WHEN ty.average_rating_this_year > 6 AND ty.average_rating_this_year <= 7 THEN 'average'::quality_enum
					        WHEN ty.average_rating_this_year <= 6 THEN 'bad'::quality_enum
      					END
    				ELSE ly.quality_class
				END	AS quality_class,				
			ty.year IS NOT NULL AS is_active,
			1974 AS year
		FROM last_year ly
		FULL OUTER JOIN this_year ty
		ON ly.actorid = ty.actorid
	)
SELECT * FROM actor_film_year;

-- DDL for `actors_history_scd` table

CREATE TABLE actors_history_scd (
	actor TEXT,
	quality_class quality_enum,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	PRIMARY KEY (actor, start_year)	
)


-- Backfill query for `actors_history_scd`
INSERT INTO actors_history_scd
WITH with_previous AS (
	SELECT 
		actor, 
		quality_class,
		is_active,
		LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY year) AS previous_quality_class,		
		LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY year) AS previous_is_active,
		year
	FROM actors
	WHERE year <= 2021
), 
	with_indicators AS (
		SELECT 
			*,
			CASE 
				WHEN quality_class <> previous_quality_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END AS change_indicator			
		FROM with_previous
	),
	with_streaks AS (
		SELECT 
			*,
		SUM(change_indicator) OVER (PARTITION BY actor ORDER BY year) AS streak_identifier
		FROM with_indicators
	)
	SELECT 	
		actor,
		quality_class,
		is_active,		
		MIN(year) as start_year,
		MAX(year) as end_year,
		2021 AS current_year
	FROM with_streaks
	GROUP BY actor, streak_identifier, is_active, quality_class
	ORDER BY actor, streak_identifier

-- Incremental query for `actors_history_scd`
CREATE TYPE scd_type AS (
	quality_class quality_enum,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
)

WITH last_year_scd AS (
	SELECT * FROM actors_history_scd 
	WHERE current_year = 2021
	AND end_year = 2021
),
	historical_scd AS (
		SELECT 
			actor,
			quality_class,
			is_active,
			start_year,
			end_year			
		FROM actors_history_scd 
		WHERE current_year = 2021
		AND end_year < 2021
	),
	this_year_data AS (
		SELECT * FROM actors
		WHERE year = 2022
	),
	unchanged_records AS (
		SELECT 
			ty.actor,
			ty.quality_class, 
			ty.is_active,
			ly.start_year, 
			ty.year as end_season
		FROM this_year_data ty
		JOIN last_year_scd ly
		ON ty.actor = ly.actor
		WHERE ty.quality_class = ly.quality_class
		AND ty.is_active = ly.is_active
	),
	changed_records AS (	
		SELECT 
			ty.actor,
			UNNEST(ARRAY[
				ROW(
				 ly.quality_class,
				 ly.is_active,
				 ly.start_year,
				 ly.end_year
				)::scd_type,
				ROW(
				 ty.quality_class,
				 ty.is_active,
				 ty.year,
				 ty.year
				)::scd_type
			]) as records		
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
		ON ty.actor = ly.actor
		WHERE (ty.quality_class <> ly.quality_class
		OR ty.is_active <> ly.is_active)		
	),
	unnested_changed_records AS (
		SELECT 
			actor,
			(records::scd_type).quality_class,
			(records::scd_type).is_active,
			(records::scd_type).start_year,
			(records::scd_type).end_year
		FROM changed_records		
	),
	new_records AS (
		SELECT 
			ty.actor,
			ty.quality_class,
			ty.is_active,
			ty.year AS start_year,
			ty.year AS end_year
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
		ON ty.actor = ly.actor
		WHERE ly.actor IS NULL
	)


SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * FROM unnested_changed_records	

UNION ALL

SELECT * FROM new_records