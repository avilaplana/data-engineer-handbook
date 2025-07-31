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
                .csv("../../data/medals.csv") \
                .where(col('name').isNotNull())

# Extract maps data set
maps_df = spark \
            .read \
            .option("header", "true") \
            .csv("../../data/maps.csv") \
            .where(col('name').isNotNull())

# - Explicitly broadcast JOINs `medals` and `maps`
medals_and_maps_df = medals_df.join(maps_df,on="name",how="left")
broadcast(medals_and_maps_df)

# - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets

# Extract match_details data set
match_details_df = spark \
                    .read \
                    .option("header", "true") \
                    .csv("../../data/match_details.csv")

# print("match_details_df")
# match_details_df.orderBy(col("match_id")).show(20)

# Extract matches data set
matches_df = spark \
                .read \
                .option("header","true") \
                .csv("../../data/matches.csv")

# print("matches_df")
# matches_df.orderBy(col("match_id")).show(20)

# Extract medal_matches_players data set
medal_matches_players_df = spark \
                            .read \
                            .option("header","true") \
                            .csv("../../data/medals_matches_players.csv") \
                            .withColumnRenamed("player_gamertag","player_gamertag_medal")

# print("medal_matches_players_df")
# medal_matches_players_df.orderBy(col("match_id")).show(20)

# Alias your DataFrames
matches_alias_df = matches_df.alias("m")
match_details_alias_df = match_details_df.alias("md")
medal_matches_players_alias_df = medal_matches_players_df.alias("mmp")

matches_join_df = matches_alias_df \
                    .join(match_details_alias_df, on="match_id", how="inner") \
                    .join(medal_matches_players_alias_df, on="match_id", how="inner")


repartition_matches_join_df = matches_join_df \
                            .repartition(16, col("m.match_id"))

# repartition_matches_join_df.printSchema()

# - Aggregate the joined data frame to figure out questions like:
#     - Which player averages the most kills per game?
            # note: interesting fields: player_gamertag, player_total_kills

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
    WITH map_mach_with_row_number (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY match_id, mapid ORDER BY match_id) AS row_num    
    FROM repartition_matches_join_df        
), map_match_deduped AS (
    SELECT 
        match_id, 
        mapid
    FROM map_mach_with_row_number
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
#   - With the aggregated data set
#     - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)