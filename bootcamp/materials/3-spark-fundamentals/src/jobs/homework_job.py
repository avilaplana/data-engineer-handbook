from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

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


# Extract matches data set
matches_df = spark \
                .read \
                .option("header","true") \
                .csv("../../data/matches.csv")

# Extract medal_matches_players data set
medal_matches_players_df = spark \
                            .read \
                            .option("header","true") \
                            .csv("../../data/medals_matches_players.csv")

matches_join_df = matches_df \
                    .join(match_details_df, on="match_id", how="inner") \
                    .join(medal_matches_players_df, on="match_id", how="inner")

sorted_matches_join_df = matches_join_df \
                            .repartition(16, col("match_id"))

# - Aggregate the joined data frame to figure out questions like:
#     - Which player averages the most kills per game?


#     - Which playlist gets played the most?
#     - Which map gets played the most?
#     - Which map do players get the most Killing Spree medals on?
#   - With the aggregated data set
#     - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)