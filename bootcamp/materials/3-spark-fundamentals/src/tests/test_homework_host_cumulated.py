from chispa.dataframe_comparer import *
from datetime import date, datetime

from ..jobs.homework_host_cumulated_job import do_hosts_cumulated_transformation
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, DateType, StringType, ArrayType

Event = namedtuple("Event", "host event_time")
HostsCumulated = namedtuple("HostsCumulated", "host host_activity_datelist date")

def test_host_cumulates_first_day_month(spark):
    events_date = [
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 1, 0, 0, 1),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 1, 0, 0, 2),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 1, 0, 0, 3),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 2, 0, 1, 1),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 2, 1, 0, 2),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 1, 1, 0, 1),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 1, 3, 0, 2),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 2, 4, 0, 3),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 2, 5, 0, 3),
        )
    ]

    hosts_cumulated_data = []
    hosts_cumulated_data_schema = StructType([
        StructField("host", StringType(), False),
        StructField("host_activity_datelist", ArrayType(DateType()), False),
        StructField("date", DateType(), False)
    ])

    source_events_df = spark.createDataFrame(events_date)
    source_hosts_cumulated_df = spark.createDataFrame(hosts_cumulated_data, hosts_cumulated_data_schema)
    expected_values = [
        HostsCumulated(
            host = 'host_1',
            host_activity_datelist = [date(2025,1,1)],
            date = date(2025,1,1),
            ),
        HostsCumulated(
            host='host_2',
            host_activity_datelist=[date(2025, 1, 1)],
            date=date(2025, 1, 1),
        )
    ]
    actual_df = do_hosts_cumulated_transformation(spark, source_events_df, source_hosts_cumulated_df, '2024-12-31', '2025-01-01')

    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)

def test_host_cumulates_second_day_month(spark):
    events_date = [
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 1, 0, 0, 1),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 1, 0, 0, 2),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 1, 0, 0, 3),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 2, 0, 1, 1),
        ),
        Event(
            host='host_1',
            event_time=datetime(2025, 1, 2, 1, 0, 2),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 1, 1, 0, 1),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 1, 3, 0, 2),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 2, 4, 0, 3),
        ),
        Event(
            host='host_2',
            event_time=datetime(2025, 1, 2, 5, 0, 3),
        )
    ]

    hosts_cumulated_data = [
        HostsCumulated(
            host='host_1',
            host_activity_datelist=[date(2025, 1, 1)],
            date=date(2025, 1, 1),
        ),
        HostsCumulated(
            host='host_2',
            host_activity_datelist=[date(2025, 1, 1)],
            date=date(2025, 1, 1),
        )
    ]
    hosts_cumulated_data_schema = StructType([
        StructField("host", StringType(), False),
        StructField("host_activity_datelist", ArrayType(DateType()), False),
        StructField("date", DateType(), False)
    ])

    source_events_df = spark.createDataFrame(events_date)
    source_hosts_cumulated_df = spark.createDataFrame(hosts_cumulated_data, hosts_cumulated_data_schema)
    expected_values = [
        HostsCumulated(
            host = 'host_1',
            host_activity_datelist = [date(2025,1,2), date(2025,1,1)],
            date = date(2025,1,2),
            ),
        HostsCumulated(
            host='host_2',
            host_activity_datelist=[date(2025,1,2), date(2025,1,1)],
            date=date(2025, 1, 2),
        )
    ]
    actual_df = do_hosts_cumulated_transformation(spark, source_events_df, source_hosts_cumulated_df, '2025-01-01', '2025-01-02')

    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)
