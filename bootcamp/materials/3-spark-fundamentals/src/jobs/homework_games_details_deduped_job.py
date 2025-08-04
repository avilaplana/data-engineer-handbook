from pyspark.sql import SparkSession

def do_game_details_deduped_transformation(spark, dataframe_game_details, dataframe_game):
    query = """
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
    """
    dataframe_game_details.createOrReplaceTempView("game_details")
    dataframe_game.createOrReplaceTempView("games")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("game_details_scd") \
      .getOrCreate()
    output_df = do_game_details_deduped_transformation(spark, spark.table("game_details"), spark.table("games"))
    output_df.write.mode("overwrite").insertInto("game_details_agg")