from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# Spark Fundamentals Week

# - match_details
#   - a row for every players performance in a match
# - matches
#   - a row for every match
# - medals_matches_players
#   - a row for every medal type a player gets in a match
# - medals
#   - a row for every medal type
#
#
# Your goal is to make the following things happen:

prefix_folder="../../data/"
spark = SparkSession.builder \
            .master("local") \
            .appName("homework") \
            .getOrCreate()

# - Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Extract medals data set
medals_df = spark \
                .read \
                .option("header", "true") \
                .csv(prefix_folder + "medals.csv") \
                .where(col('name').isNotNull())

# Extract maps data set
maps_df = spark \
            .read \
            .option("header", "true") \
            .csv(prefix_folder + "maps.csv") \
            .where(col('name').isNotNull())

# - Explicitly broadcast JOINs `medals` and `maps`
medals_and_maps_df = medals_df.join(maps_df,on="name",how="left")
broadcast(medals_and_maps_df)

# - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets

# Extract match_details data set
match_details_df = spark \
                    .read \
                    .option("header", "true") \
                    .csv(prefix_folder + "match_details.csv")

# Extract matches data set
matches_df = spark \
                .read \
                .option("header","true") \
                .csv(prefix_folder + "matches.csv")

# Extract medal_matches_players data set
medal_matches_players_df = spark \
                            .read \
                            .option("header","true") \
                            .csv(prefix_folder + "medals_matches_players.csv") \
                            .withColumnRenamed("player_gamertag","player_gamertag_medal")

# Alias your DataFrames
matches_alias_df = matches_df.alias("m")
match_details_alias_df = match_details_df.alias("md")
medal_matches_players_alias_df = medal_matches_players_df.alias("mmp")

matches_join_df = matches_alias_df \
                    .join(match_details_alias_df, on="match_id", how="inner") \
                    .join(medal_matches_players_alias_df, on="match_id", how="inner")


repartition_matches_join_df = matches_join_df \
                            .repartition(16, col("m.match_id"))

matches_concise_df = repartition_matches_join_df.select("match_id", "mapid", "playlist_id", "player_gamertag", "player_total_kills", "player_gamertag_medal", "medal_id", "count")
matches_concise_df.printSchema()

spark.sql("DROP TABLE IF EXISTS bootcamp.matches")

matches_query = """
CREATE TABLE IF NOT EXISTS bootcamp.matches (
    match_id STRING,
    mapid STRING,
    playlist_id STRING,
    player_gamertag STRING,
    player_total_kills STRING,
    player_gamertag_medal STRING,
    medal_id STRING,
    count STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
"""

spark.sql(matches_query)
matches_concise_df.write.mode("overwrite").saveAsTable("bootcamp.matches")


# - Aggregate the joined data frame to figure out questions like:
#     - Which player averages the most kills per game?

repartition_matches_join_df.createOrReplaceTempView("repartition_matches_join_df")

player_averages_the_most_kills_per_game_query = """
WITH players_match_with_row_number (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY match_id, player_gamertag ORDER BY match_id) AS row_num    
    FROM repartition_matches_join_df        
), players_match_deduped AS (
    SELECT *
    FROM players_match_with_row_number
    WHERE row_num = 1                
    )
     SELECT 
        player_gamertag AS player_name,
        COUNT(1) AS count,
        AVG(player_total_kills) AS average_kills,
        SUM(player_total_kills) AS total_kills
    FROM players_match_deduped 
    GROUP BY player_gamertag      
    ORDER BY count DESC
    LIMIT 1 
"""

player_averages_the_most_kills_per_game_df = spark.sql(player_averages_the_most_kills_per_game_query)
player_averages_the_most_kills_per_game_df.show(15)

#     - Which playlist gets played the most?
playlist_gets_played_the_most_question = """
    WITH playlist_match_with_row_number (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY match_id, playlist_id ORDER BY match_id) AS row_num    
    FROM repartition_matches_join_df        
), playlist_match_deduped AS (
    SELECT 
        match_id, 
        playlist_id
    FROM playlist_match_with_row_number
    WHERE row_num = 1                
    )        

    SELECT 
        playlist_id, 
        COUNT(1) AS count
    FROM playlist_match_deduped
    GROUP BY playlist_id
    ORDER BY count DESC
    LIMIT 1    
"""

playlist_gets_played_the_most_question_df = spark.sql(playlist_gets_played_the_most_question)
playlist_gets_played_the_most_question_df.show(15)

#     - Which map gets played the most?
map_gets_played_the_most_question = """
    WITH map_match_with_row_number (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY match_id, mapid ORDER BY match_id) AS row_num    
    FROM repartition_matches_join_df        
), map_match_deduped AS (
    SELECT 
        match_id, 
        mapid
    FROM map_match_with_row_number
    WHERE row_num = 1                
    )        

    SELECT 
        mapid, 
        COUNT(1) AS count
    FROM map_match_deduped
    GROUP BY mapid
    ORDER BY count DESC
    LIMIT 1    
"""

map_gets_played_the_most_question_df = spark.sql(map_gets_played_the_most_question)
map_gets_played_the_most_question_df.show(15)

#     - Which map do players get the most Killing Spree medals on?
medals_df.createOrReplaceTempView("medals_df")

map_do_players_get_the_most_killing_spree_medals_question = """
WITH match_with_players_filtered (
    SELECT  
        match_id,
        mapid, 
        player_gamertag, 
        player_total_kills,
        medal_id,
        count 
    FROM repartition_matches_join_df
    WHERE player_gamertag = player_gamertag_medal    
), match_killing_spreee_medals AS (
    SELECT m.*, med.name
    FROM match_with_players_filtered AS m 
    JOIN medals_df AS med ON m.medal_id = med.medal_id
    WHERE med.name = 'Killing Spree'
    ORDER BY m.match_id
), map_aggregates AS (
    SELECT 
        mapid,
        SUM(player_total_kills) AS total_kills
    FROM match_killing_spreee_medals
    GROUP BY mapid
    ORDER BY total_kills DESC
    LIMIT 1
)
    
SELECT * FROM map_aggregates
"""

map_do_players_get_the_most_killing_spree_medals_question_df = spark.sql(map_do_players_get_the_most_killing_spree_medals_question)
map_do_players_get_the_most_killing_spree_medals_question_df.show(15, truncate=False)

#   - With the aggregated data set
#     - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)


spark.sql("DROP TABLE IF EXISTS bootcamp.matches_sorted")

matches_query = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_sorted (
    match_id STRING,
    mapid STRING,
    playlist_id STRING,
    player_gamertag STRING,
    player_total_kills STRING,
    player_gamertag_medal STRING,
    medal_id STRING,
    count STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
"""

matches_concise_df \
    .sortWithinPartitions("playlist_id", "mapid") \
    .write.mode("overwrite") \
    .saveAsTable("bootcamp.matches_sorted")

sql_query_to_check_files_size = """
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM demo.bootcamp.matches.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM demo.bootcamp.matches_sorted.files
"""

spark.sql(sql_query_to_check_files_size).show()