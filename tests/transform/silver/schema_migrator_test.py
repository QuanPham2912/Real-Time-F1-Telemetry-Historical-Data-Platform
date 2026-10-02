import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, lit, struct, when
from pyspark.sql.types import (
	FloatType,
	IntegerType,
	StringType,
	StructField,
	StructType,
)

import transform.silver.schema_migrator as schema_migrator_module
from metadata.f1_topic import F1Topic
from transform.silver.schema_migrator import DerivedLogic


@pytest.fixture(scope="module")
def spark():
	os.environ["PYSPARK_PYTHON"] = sys.executable
	session = (
		SparkSession.builder.master("local[1]")
		.appName("schema-migrator-tests")
		.config("spark.ui.enabled", "false")
		.config("spark.sql.session.timeZone", "UTC")
		.getOrCreate()
	)
	yield session
	session.stop()


@pytest.mark.parametrize(
	("race_time", "expected_date"),
	[
		("15:00:00Z", "2024-03-02 15:00:00"),
		(None, "2024-03-02 00:00:00"),
	],
)
def test_transform_dim_race_combines_date_and_optional_time(
	spark, race_time, expected_date
):
	circuit_schema = StructType(
		[
			StructField("circuitId", StringType()),
			StructField("circuitName", StringType()),
			StructField("Location", StructType([])),
		]
	)
	race_schema = StructType(
		[
			StructField("race_id", StringType()),
			StructField("season", IntegerType()),
			StructField("round", IntegerType()),
			StructField("Circuit", circuit_schema),
			StructField("raceName", StringType()),
			StructField("date", StringType()),
			StructField("time", StringType()),
			StructField("source", StringType()),
		]
	)
	race = {
		"race_id": "2024_1",
		"season": 2024,
		"round": 1,
		"Circuit": {"circuitId": "bahrain", "circuitName": "Bahrain", "Location": {}},
		"raceName": "Bahrain Grand Prix",
		"date": "2024-03-02",
		"time": race_time,
		"source": "jolpica_api",
	}

	result = DerivedLogic.transform_dim_race(
		{F1Topic.JOLPICA_RACE: spark.createDataFrame([race], race_schema)}
	).select("race_id", date_format("date", "yyyy-MM-dd HH:mm:ss").alias("date")).first()

	assert result.race_id == "2024_1"
	assert result.date == expected_date


def test_transform_dim_session_uses_race_timestamp(spark):
	schema = StructType(
		[
			StructField("race_id", StringType()),
			StructField("date", StringType()),
			StructField("time", StringType()),
			StructField("source", StringType()),
			StructField("season", IntegerType()),
		]
	)
	race = spark.createDataFrame(
		[("2024_1", "2024-03-02", "15:00:00Z", "jolpica_api", 2024)],
		schema,
	)

	result = DerivedLogic.transform_dim_session(
		{F1Topic.JOLPICA_RACE: race}
	).select(
		"session_id",
		date_format("date", "yyyy-MM-dd HH:mm:ss").alias("date"),
	).first()

	assert result.session_id == "2024_1R"
	assert result.date == "2024-03-02 15:00:00"


def test_transform_dim_race_result_statsf1_maps_case_insensitive_status(spark):
	schema = StructType(
		[
			StructField("Race_id", StringType()),
			StructField("Position", StringType()),
			StructField("Driver_number", StringType()),
			StructField("Driver", StringType()),
			StructField("Chassis", StringType()),
			StructField("Engine_manufacturer", StringType()),
			StructField("Total_lap", IntegerType()),
			StructField("Race_time", StringType()),
		]
	)
	rows = [
		("2024_1", "1", "1", "Winner", "A", "E", 10, "1h 20m 30s"),
		("2024_1", "AB", "2", "Retired", "B", "E", 8, "ab"),
	]

	result = DerivedLogic.transform_dim_race_result_statsf1(
		{F1Topic.STATSF1_RACE_RESULT: spark.createDataFrame(rows, schema)}
	).orderBy("Driver_number").collect()

	assert result[0].Status == "FINISHED"
	assert result[0].is_completed_full_laps is True
	assert result[0].total_race_time_ms == 4_830_000.0
	assert result[1].Status == "DNF"
	assert result[1].Description == "ab"
	assert result[1].is_completed_full_laps is False


def test_transform_fact_result_projects_grid_column(spark):
	average_speed_schema = StructType(
		[StructField("units", StringType()), StructField("speed", FloatType())]
	)
	fastest_lap_schema = StructType(
		[
			StructField("rank", IntegerType()),
			StructField("lap", IntegerType()),
			StructField("Time", StructType([StructField("time", StringType())])),
			StructField("AverageSpeed", average_speed_schema),
		]
	)
	schema = StructType(
		[
			StructField("race_id", StringType()),
			StructField("Driver", StructType([StructField("driverId", StringType())])),
			StructField("Constructor", StructType([StructField("constructorId", StringType())])),
			StructField("number", IntegerType()),
			StructField("position", IntegerType()),
			StructField("points", FloatType()),
			StructField("grid", IntegerType()),
			StructField("laps", IntegerType()),
			StructField("status", StringType()),
			StructField("Time", StructType([StructField("millis", FloatType()), StructField("time", StringType())])),
			StructField("FastestLap", fastest_lap_schema),
			StructField("source", StringType()),
		]
	)
	row = {
		"race_id": "2024_1",
		"Driver": {"driverId": "driver-1"},
		"Constructor": {"constructorId": "constructor-1"},
		"number": 1,
		"position": 1,
		"points": 25.0,
		"grid": 2,
		"laps": 10,
		"status": "Finished",
		"Time": {"millis": 4830000.0, "time": "1:20:30"},
		"FastestLap": {
			"rank": 1,
			"lap": 5,
			"Time": {"time": "1:20.000"},
			"AverageSpeed": {"units": " kph", "speed": 200.0},
		},
		"source": "jolpica_api",
	}

	result = DerivedLogic.transform_fact_result(
		{F1Topic.JOLPICA_RACE_RESULT: spark.createDataFrame([row], schema)}
	).first()

	assert result.grid == 2
	assert result.driver_id == "driver-1"
	assert result.constructor_id == "constructor-1"


def test_transform_xwalk_constructor_uses_raw_statsf1_schema(spark, monkeypatch):
	constructor_schema = StructType(
		[
			StructField("constructorId", StringType()),
			StructField("name", StringType()),
			StructField("nationality", StringType()),
			StructField("source", StringType()),
		]
	)
	statsf1_schema = StructType(
		[
			StructField("Constructor", StringType()),
			StructField("Nation", StringType()),
			StructField("Started_time", IntegerType()),
			StructField("Source", StringType()),
		]
	)
	constructors = spark.createDataFrame(
		[("ferrari", "Ferrari", "Italian", "jolpica_api")], constructor_schema
	)
	statsf1 = spark.createDataFrame(
		[("Ferrari", "Italian", 1950, "Stats_F1")], statsf1_schema
	)

	def pandas_udf_stub(return_type):
		def decorate(function):
			def build_match_result(name_column, nation_column):
				return struct(
					name_column.alias("master_key"),
					(lit(1.0) + (nation_column.isNull().cast("float"))).alias("confidence_score"),
				)

			return build_match_result

		return decorate

	monkeypatch.setattr(schema_migrator_module, "pandas_udf", pandas_udf_stub)

	result = DerivedLogic.transform_xwalk_constructor(
		{
			F1Topic.JOLPICA_CONSTRUCTOR: constructors,
			F1Topic.STATSF1_CONSTRUCTOR_STATSF1: statsf1,
		}
	).first()

	assert result.source_native_key == "Ferrari"
	assert result.master_key == "Ferrari"
	assert result.confidence_score == pytest.approx(1.0)


def test_transform_xwalk_driver_resolves_constructor_and_season(spark, monkeypatch):
	driver_schema = StructType(
		[
			StructField("Driver", StringType()),
			StructField("Constructor", StringType()),
			StructField("Season", IntegerType()),
			StructField("Source", StringType()),
		]
	)
	driver_statsf1 = spark.createDataFrame(
		[("Alex Smith", "Ferrari", 2022, "Stats_F1")], driver_schema
	)
	xwalk_schema = StructType(
		[
			StructField("source", StringType()),
			StructField("source_native_key", StringType()),
			StructField("master_key", StringType()),
			StructField("confidence_score", FloatType()),
		]
	)
	xwalk_constructor = spark.createDataFrame(
		[("Stats_F1", "Ferrari", "ferrari", 1.0)], xwalk_schema
	)
	dim_rows = [
		SimpleNamespace(
			driverId="wrong-driver",
			driver_name="Alex Smith",
			constructorId="ferrari",
			season=2021,
		),
		SimpleNamespace(
			driverId="right-driver",
			driver_name="Alex Smith",
			constructorId="ferrari",
			season=2022,
		),
		SimpleNamespace(
			driverId="right-driver",
			driver_name="Alex Smith",
			constructorId="mclaren",
			season=2023,
		),
	]
	collected_rows = SimpleNamespace(collect=lambda: dim_rows)
	distinct_rows = SimpleNamespace(distinct=lambda: collected_rows)
	fact_result = SimpleNamespace(select=lambda *columns: distinct_rows)

	def pandas_udf_stub(return_type):
		def decorate(function):
			def build_match_result(name_column, constructor_column, season_column):
				matches = function(
					pd.Series(["Alex Smith"]),
					pd.Series(["ferrari"]),
					pd.Series([2022]),
				)
				return struct(
					lit(matches.iloc[0]["master_key"]).alias("master_key"),
					when(constructor_column.isNotNull(), lit(float(matches.iloc[0]["confidence_score"])))
					.otherwise(lit(0.0))
					.alias("confidence_score"),
				)

			return build_match_result

		return decorate

	monkeypatch.setattr(schema_migrator_module, "pandas_udf", pandas_udf_stub)

	result = DerivedLogic.transform_xwalk_driver(
		{
			F1Topic.JOLPICA_RACE_RESULT: fact_result,
			F1Topic.STATSF1_DRIVER_STATSF1: driver_statsf1,
			"XWALK_CONSTRUCTOR": xwalk_constructor,
		}
	).first()

	assert result.Source == "Stats_F1"
	assert result.source_native_key == "Alex Smith"
	assert result.master_key == "right-driver"
	assert result.confidence_score == pytest.approx(1.0)
