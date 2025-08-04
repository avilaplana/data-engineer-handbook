from chispa.dataframe_comparer import *
from datetime import date
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

from ..jobs.homework_games_details_deduped_job import do_game_details_deduped_transformation
from collections import namedtuple

Game = namedtuple("Game", "game_id home_team_id game_date_est season")
GameDetails = namedtuple("GameDetails", "game_id team_id, player_id player_name")
GameDetailsDeduped = namedtuple("GameDetailsDeduped",
                                "game_date_est season home_team_id game_id team_id player_id player_name row_num")


expected_schema = StructType([
    StructField("game_date_est", DateType(), True),
    StructField("season", LongType(), True),
    StructField("home_team_id", LongType(), True),
    StructField("game_id", LongType(), True),
    StructField("team_id", LongType(), True),
    StructField("player_id", LongType(), True),
    StructField("player_name", StringType(), True),
    StructField("row_num", IntegerType(), False),
])

def test_game_details_deduped(spark):
    game_data = [
        Game(
            game_id=1,
            home_team_id=1,
            game_date_est=date(2015,1,1),
            season=1
        ),
        Game(
            game_id=2,
            home_team_id=1,
            game_date_est=date(2015,1,3),
            season=1
        ),
        Game(
            game_id=3,
            home_team_id=1,
            game_date_est=date(2015,1,5),
            season=1
        ),
        Game(
            game_id=4,
            home_team_id=1,
            game_date_est=date(2015,1,7),
            season=1
        ),
        Game(
            game_id=5,
            home_team_id=1,
            game_date_est=date(2015,1,9),
            season=1
        )
    ]

    game_details_data = [
        GameDetails(game_id=1,
                    team_id=1,
                    player_id=1,
                    player_name="player_1") ,
        GameDetails(game_id=1, # duplicated
                    team_id=1,
                    player_id=1,
                    player_name="player_1"),
        GameDetails(game_id=2,
                    team_id=1,
                    player_id=1,
                    player_name="player_1"),
        GameDetails(game_id=3,
                    team_id=1,
                    player_id=1,
                    player_name="player_1"),
        GameDetails(game_id=3, # duplicated
                    team_id=1,
                    player_id=1,
                    player_name="player_1"),
        GameDetails(game_id=4,
                    team_id=1,
                    player_id=1,
                    player_name="player_1"),
        GameDetails(game_id=5,
                    team_id=1,
                    player_id=1,
                    player_name="player_1"),
    ]

    source_game_df = spark.createDataFrame(game_data)
    source_game_details_df = spark.createDataFrame(game_details_data)
    expected_values = [
        GameDetailsDeduped(
                    game_date_est=date(2015, 1, 1),
                    season=1,
                    home_team_id=1,
                    game_id=1,
                    team_id=1,
                    player_id=1,
                    player_name="player_1",
                    row_num=1
                    ),
        GameDetailsDeduped(
                    game_date_est=date(2015, 1, 3),
                    season=1,
                    home_team_id=1,
                    game_id=2,
                    team_id=1,
                    player_id=1,
                    player_name="player_1",
                    row_num=1),
        GameDetailsDeduped(
                    game_date_est=date(2015, 1, 5),
                    season=1,
                    home_team_id=1,
                    game_id=3,
                    team_id=1,
                    player_id=1,
                    player_name="player_1",
                    row_num=1),
        GameDetailsDeduped(
                    game_date_est=date(2015, 1, 7),
                    season=1,
                    home_team_id=1,
                    game_id=4,
                    team_id=1,
                    player_id=1,
                    player_name="player_1",
                    row_num=1),
        GameDetailsDeduped(
            game_date_est=date(2015, 1, 9),
            season=1,
            home_team_id=1,
            game_id=5,
            team_id=1,
            player_id=1,
            player_name="player_1",
            row_num=1),
    ]
    actual_df = do_game_details_deduped_transformation(spark, source_game_details_df, source_game_df)

    expected_df = spark.createDataFrame(expected_values, expected_schema)
    assert_df_equality(actual_df, expected_df)
