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

first_question_query = """
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
    
    SELECT * FROM players_match_deduped
"""

q_df = spark.sql(first_question_query)

q_df.show(15)

#     - Which playlist gets played the most?
#     - Which map gets played the most?
#     - Which map do players get the most Killing Spree medals on?
#   - With the aggregated data set
#     - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)