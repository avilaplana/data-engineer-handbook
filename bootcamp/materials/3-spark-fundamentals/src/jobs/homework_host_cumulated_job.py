from pyspark.sql import SparkSession

def do_hosts_cumulated_transformation(spark, dataframe_events, dataframe_hosts_cumulated, ds_yesterday, ds_today):
    query = f"""
        WITH yesterday AS (
            SELECT * 
            FROM hosts_cumulated
            WHERE date = DATE('{ds_yesterday}')
        ), today AS (
            SELECT 
                host,
                DATE(CAST(event_time AS TIMESTAMP)) AS date_active	
            FROM events
            WHERE 
                DATE(CAST(event_time AS TIMESTAMP)) = DATE('{ds_today}')		
            GROUP BY host, DATE(CAST(event_time AS TIMESTAMP))
        )
        
        SELECT 
            COALESCE(t.host, y.host) AS host,
            CASE 
                WHEN y.host_activity_datelist IS NULL THEN array(t.date_active)
                WHEN t.date_active IS NULL THEN y.host_activity_datelist
                ELSE concat(array(t.date_active), y.host_activity_datelist)
            END AS host_activity_datelist,
            COALESCE(t.date_active, y.date + INTERVAL 1 day) AS date	
        FROM today t 
        FULL OUTER JOIN yesterday y
        ON t.host = y.host

    """
    dataframe_events.createOrReplaceTempView("events")
    dataframe_hosts_cumulated.createOrReplaceTempView("hosts_cumulated")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("game_details_scd") \
      .getOrCreate()
    output_df = do_hosts_cumulated_transformation(spark, spark.table("events"), spark.table("hosts_cumulated"), '2023-01-30', '2023-01-31')
    output_df.write.mode("overwrite").insertInto("host_cumulated_agg")