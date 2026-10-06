import os
import sys

import pytest
from pyspark.sql import SparkSession, functions as F

from transform.gold.gold_logic import Gold_logic


@pytest.fixture(scope="module")
def spark():
	previous_python = os.environ.get("PYSPARK_PYTHON")
	os.environ["PYSPARK_PYTHON"] = sys.executable
	session = (
		SparkSession.builder.master("local[1]")
		.appName("gold-logic-tests")
		.config("spark.ui.enabled", "false")
		.config("spark.driver.host", "127.0.0.1")
		.config("spark.driver.bindAddress", "127.0.0.1")
		.getOrCreate()
	)
	session.sparkContext.setLogLevel("ERROR")
	yield session
	session.stop()
	if previous_python is None:
		os.environ.pop("PYSPARK_PYTHON", None)
	else:
		os.environ["PYSPARK_PYTHON"] = previous_python


def test_normalize_lowercases_trims_and_collapses_whitespace(spark):
	source = spark.range(1).select(F.lit("  MAX\t  Verstappen  ").alias("name"))

	result = source.select(Gold_logic.normalize(F.col("name")).alias("normalized"))

	assert [row.normalized for row in result.collect()] == ["max verstappen"]


def test_add_unknown_row_uses_unknown_values_and_null_for_non_string_fields(spark):
	source = spark.range(1).select(
		F.lit(44).cast("long").alias("driver_key"),
		F.lit("verstappen").alias("driver_name"),
		F.lit(2024).cast("int").alias("season"),
	)

	result = Gold_logic.add_unknown_row(source, "driver_key")
	rows = {row.driver_key: row.asDict() for row in result.collect()}

	assert rows[44] == {
		"driver_key": 44,
		"driver_name": "verstappen",
		"season": 2024,
	}
	assert rows[-1] == {
		"driver_key": -1,
		"driver_name": "UNKNOWN",
		"season": None,
	}


def test_transform_dim_driver_hashes_ids_and_appends_unknown_row(spark):
	source = spark.range(1).select(
		F.lit("max_verstappen").alias("driver_id"),
		F.lit("VER").alias("driver_tla"),
		F.lit("Max").alias("given_name"),
		F.lit("Verstappen").alias("family_name"),
		F.lit("1997-09-30").cast("date").alias("date_of_birth"),
		F.lit("Dutch").alias("nationality"),
		F.lit(33).alias("permanent_number"),
	)

	result = Gold_logic.transform_dim_driver({"DIM_DRIVER": source})
	rows = {row.driver_key: row.asDict() for row in result.collect()}
	expected_key = source.select(F.xxhash64("driver_id").alias("key")).first().key

	assert result.columns == [
		"driver_key",
		"driver_id",
		"driver_tla",
		"given_name",
		"family_name",
		"date_of_birth",
		"nationality",
	]
	assert rows[expected_key]["driver_id"] == "max_verstappen"
	assert rows[expected_key]["driver_tla"] == "VER"
	assert rows[-1]["driver_id"] == "UNKNOWN"
	assert rows[-1]["driver_tla"] == "UNKNOWN"


def test_transform_dim_session_joins_race_and_circuit_details(spark):
	session = spark.range(1).select(
		F.lit("race_2024_1_R").alias("session_id"),
		F.lit("race_2024_1").alias("race_id"),
		F.lit("2024-03-02").cast("date").alias("date"),
	)
	race = spark.range(1).select(
		F.lit("race_2024_1").alias("race_id"),
		F.lit("bahrain").alias("circuit_id"),
		F.lit("Bahrain Grand Prix").alias("race_name"),
		F.lit(2024).alias("season"),
		F.lit(1).alias("round"),
	)
	circuit = spark.range(1).select(
		F.lit("bahrain").alias("circuit_id"),
		F.lit(26.0325).alias("lat"),
		F.lit(50.5106).alias("long"),
		F.lit("Sakhir").alias("locality"),
		F.lit("Bahrain").alias("country"),
		F.lit("Bahrain International Circuit").alias("circuit_name"),
	)

	result = Gold_logic.transform_dim_session(
		{"DIM_SESSION": session, "DIM_RACE": race, "DIM_CIRCUIT": circuit}
	).first()

	assert result.session_key == session.select(F.xxhash64("session_id")).first()[0]
	assert result.session_type == "Race"
	assert result.session_date.isoformat() == "2024-03-02"
	assert result.race_name == "Bahrain Grand Prix"
	assert result.season == 2024
	assert result.circuit_name == "Bahrain International Circuit"
	assert result.country == "Bahrain"


def test_transform_dim_constructor_matches_normalized_crosswalk_names(spark):
	constructor = spark.range(1).select(
		F.lit("red_bull").alias("constructor_id"),
		F.lit("Red Bull Racing").alias("name"),
		F.lit("Austrian").alias("nationality"),
	)
	crosswalk = spark.range(1).select(
		F.lit("red_bull").alias("master_key"),
		F.lit("RED   BULL RACING").alias("source_native_key"),
		F.lit("StatsF1").alias("source"),
	)
	statsf1 = spark.range(1).select(
		F.lit("red bull racing").alias("constructor_name"),
		F.lit("statsf1").alias("source"),
		F.lit(2005).alias("started_time"),
	)

	result = Gold_logic.transform_dim_constructor(
		{
			"DIM_CONSTRUCTOR": constructor,
			"XWALK_CONSTRUCTOR": crosswalk,
			"DIM_CONSTRUCTOR_STATSF1": statsf1,
		}
	)
	rows = result.collect()
	known = next(row for row in rows if row.constructor_id == "red_bull")
	unknown = next(row for row in rows if row.constructor_key == -1)

	assert known.name == "Red Bull Racing"
	assert known.started_time == 2005
	assert known.constructor_key == constructor.select(
		F.xxhash64("constructor_id")
	).first()[0]
	assert unknown.constructor_key == -1
	assert unknown.name == "UNKNOWN"


def test_transform_dim_engine_supplier_projects_fields_and_adds_unknown_row(spark):
	source = spark.range(1).select(
		F.lit("Honda").alias("engine_manufacturer"),
		F.lit("Japan").alias("nation"),
		F.lit(1964).alias("started_time"),
		F.lit("StatsF1").alias("source"),
	)

	result = Gold_logic.transform_dim_engine_supplier(
		{"DIM_ENGINE_SUPPLIER_STATSF1": source}
	)
	rows = result.collect()
	known = next(row for row in rows if row.engine_manufacturer == "Honda")
	unknown = next(row for row in rows if row.engine_manufacturer_key == -1)

	assert known.engine_manufacturer_key == source.select(
		F.xxhash64("engine_manufacturer")
	).first()[0]
	assert known.nation == "Japan"
	assert known.started_time == 1964
	assert unknown.engine_manufacturer_key == -1
	assert unknown.nation == "UNKNOWN"
	assert unknown.started_time is None


def test_transform_fact_weather_renames_time_and_hashes_session(spark):
	source = spark.range(1).select(
		F.lit("race_2024_1_R").alias("session_id"),
		F.lit(12.5).alias("time"),
		F.lit(25.0).alias("air_temp"),
		F.lit(35.0).alias("track_temp"),
		F.lit(60.0).alias("humidity"),
		F.lit(False).alias("rain_fall"),
		F.lit(3.5).alias("wind_speed"),
		F.lit(180).alias("wind_direction"),
		F.lit(1013.0).alias("pressure"),
		F.lit(2024).alias("season"),
	)

	result = Gold_logic.transform_fact_weather({"FACT_WEATHER": source})
	row = result.first()

	assert row.session_key == source.select(F.xxhash64("session_id")).first()[0]
	assert row.session_time == 12.5
	assert row.air_temp == 25.0
	assert row.rain_fall is False
	assert row.season == 2024
	assert "time" not in result.columns


def test_transform_fact_lap_estimates_missing_lap_boundaries(spark):
	fact_lap = (
		spark.range(2)
		.select(
			F.lit("race_2024_1_R").alias("session_id"),
			F.lit("VER").alias("driver_tla"),
			F.lit(1).alias("permanent_number"),
			(F.col("id") + 1).cast("int").alias("lap_number"),
			F.lit(10.0).alias("lap_time"),
			F.when(F.col("id") == 0, F.lit(None).cast("double"))
			.otherwise(F.lit(10.0))
			.alias("lap_start_time"),
			F.when(F.col("id") == 0, F.lit(10.0))
			.otherwise(F.lit(None).cast("double"))
			.alias("lap_end_time"),
			F.lit(1.0).alias("sector_1_time"),
			F.lit(2.0).alias("sector_2_time"),
			F.lit(3.0).alias("sector_3_time"),
			F.lit(1).alias("stint"),
			F.lit("SOFT").alias("compound"),
			F.lit(5).alias("tyre_life"),
			F.lit(True).alias("fresh_tyre"),
			F.lit(None).cast("double").alias("pit_in_time"),
			F.lit(None).cast("double").alias("pit_out_time"),
			F.lit("1").alias("track_status"),
			F.lit(True).alias("is_accurate"),
			F.lit(2024).alias("season"),
		)
	)
	fact_result = spark.range(1).select(
		F.lit("race_2024_1").alias("race_id"),
		F.lit(1).alias("permanent_number"),
		F.lit("max_verstappen").alias("driver_id"),
		F.lit(None).cast("long").alias("driver_key"),
		F.lit(None).cast("long").alias("constructor_id"),
	)

	result = Gold_logic.transform_fact_lap(
		{"FACT_LAP": fact_lap, "FACT_RESULT": fact_result}
	)
	rows = {row.lap_number: row for row in result.collect()}

	assert rows[1].lap_start_time == 0.0
	assert rows[1].is_lap_start_time_estimated is True
	assert rows[1].lap_end_time == 10.0
	assert rows[1].is_lap_end_time_estimated is False
	assert rows[2].lap_start_time == 10.0
	assert rows[2].is_lap_start_time_estimated is False
	assert rows[2].lap_end_time == 20.0
	assert rows[2].is_lap_end_time_estimated is True
	assert rows[1].driver_key == -1
	assert rows[1].constructor_key == -1


def test_transform_fact_telemetry_associates_points_with_lap_and_driver(spark):
	session_id = "race_2024_1_R"
	telemetry = spark.range(1).select(
		F.lit(session_id).alias("session_id"),
		F.lit(1).alias("permanent_number"),
		F.lit(15.0).alias("session_time"),
		F.lit("2024-03-02 14:30:00").cast("timestamp").alias("event_time"),
		F.lit(15.0).alias("time"),
		F.lit(320.0).alias("speed"),
		F.lit(90.0).alias("throttle"),
		F.lit(False).alias("brake"),
		F.lit(12000).alias("rpm"),
		F.lit(8).alias("n_gear"),
		F.lit(12).alias("drs"),
		F.lit(100.0).alias("distance"),
		F.lit(1.0).alias("x"),
		F.lit(2.0).alias("y"),
		F.lit(3.0).alias("z"),
		F.lit(2024).alias("season"),
	)
	fact_result = spark.range(1).select(
		F.lit("race_2024_1").alias("race_id"),
		F.lit(1).alias("permanent_number"),
		F.lit("max_verstappen").alias("driver_id"),
	)
	fact_lap = spark.range(1).select(
		F.xxhash64(F.lit(session_id)).alias("session_key"),
		F.lit(1).alias("permanent_number"),
		F.lit(3).alias("lap_number"),
		F.lit(10.0).alias("lap_start_time"),
		F.lit(20.0).alias("lap_end_time"),
	)

	result = Gold_logic.transform_fact_telemetry(
		{
			"FACT_TELEMETRY": telemetry,
			"FACT_RESULT": fact_result,
			"FACT_LAP": fact_lap,
		}
	)
	row = result.first()

	assert row.session_key == telemetry.select(F.xxhash64("session_id")).first()[0]
	assert row.driver_key == fact_result.select(
		F.xxhash64("driver_id")
	).first()[0]
	assert row.lap_number == 3
	assert row.event_time.isoformat(sep=" ") == "2024-03-02 14:30:00"
	assert row.lap_time == 15.0
	assert row.speed == 320.0
	assert row.season == 2024