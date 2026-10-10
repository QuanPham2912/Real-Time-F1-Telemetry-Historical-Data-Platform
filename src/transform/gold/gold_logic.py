from metadata.f1_topic import F1Topic
from pyspark.sql import DataFrame
from pyspark.sql.functions import lead, lag, xxhash64, current_timestamp, lower, trim, regexp_replace, pandas_udf,broadcast, concat, concat_ws, to_timestamp, lit, create_map, coalesce, regexp_extract, col, when, min as _min, max as _max, expr
from pyspark.sql.window import Window
from pyspark.sql import Column
from metadata.f1_result_status import F1ResultStatus
from itertools import chain
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from rapidfuzz import fuzz, process
import pandas as pd

session_type = "Race"

class Gold_logic:
    @staticmethod
    def normalize(column: Column) -> Column:
        return trim(regexp_replace(lower(column), r"\s+", " "))
    
    @staticmethod
    def add_unknown_row(df :DataFrame, key_col: str):
        values = []
        for field in df.schema.fields:
            if field.name == key_col:
                values.append(-1)
            elif isinstance(field.dataType, StringType):
                values.append("UNKNOWN")
            else:
                values.append(None)
        unknown_schema = StructType([
            StructField(field.name, field.dataType, True, field.metadata)
            for field in df.schema.fields
        ])
        unknown_df = df.sparkSession.createDataFrame([tuple(values)], unknown_schema)
        return df.unionByName(unknown_df)
    
    @staticmethod
    def transform_dim_session(sources :dict) -> DataFrame:
        dim_session_df = sources["DIM_SESSION"]
        dim_race_df = sources["DIM_RACE"]
        dim_circuit_df = sources["DIM_CIRCUIT"]

        result_df = dim_session_df.join(dim_race_df, on = "race_id", how = "inner") \
                                    .join(dim_circuit_df, on = "circuit_id", how = "inner") \
                                    .withColumn("session_key", xxhash64(col("session_id"))) \
                                    .select(
                                        col("session_key"),
                                        col("session_id"),
                                        lit(session_type).alias("session_type"),   #Have to change if in the future add more session
                                        dim_session_df.date.alias("session_date"),
                                        col("race_id"),
                                        col("race_name"),
                                        dim_race_df.season,
                                        dim_race_df.round,
                                        col("circuit_id"),
                                        col("circuit_name"),
                                        col("lat"),
                                        col("long"),
                                        col("locality"),
                                        col("country")
                                    )
        return result_df

    @staticmethod
    def transform_dim_driver(sources :dict) -> DataFrame:
        dim_driver_df = sources["DIM_DRIVER"]

        result_df = dim_driver_df.withColumn("driver_key", xxhash64(col("driver_id"))) \
                                    .select(
                                        col("driver_key"),
                                        col("driver_id"),
                                        col("driver_tla"),
                                        #col("permanent_number"), Because car numbers can change annually, they are accurate at the "driver × season" or "driver × race" level, but not at the "driver" level
                                        col("given_name"),
                                        col("family_name"),
                                        col("date_of_birth"),
                                        col("nationality"),
                                    )

        result_df = Gold_logic.add_unknown_row(result_df, "driver_key")
        return result_df

    @staticmethod
    def transform_dim_constructor(sources :dict) -> DataFrame:
        dim_constructor_df = sources["DIM_CONSTRUCTOR"]
        xwalk_constructor_df = sources["XWALK_CONSTRUCTOR"].withColumnRenamed("master_key", "constructor_id") \
                                                            .withColumnRenamed("source_native_key", "constructor_name")
        dim_constructor_statsf1_df = sources["DIM_CONSTRUCTOR_STATSF1"]

        result_df = dim_constructor_df.join(xwalk_constructor_df, on = "constructor_id", how = "left") \
                                    .join(dim_constructor_statsf1_df,
                                          ((Gold_logic.normalize(dim_constructor_statsf1_df.constructor_name) == Gold_logic.normalize(xwalk_constructor_df.constructor_name)) &
                                           (Gold_logic.normalize(xwalk_constructor_df.source) == Gold_logic.normalize(dim_constructor_statsf1_df.source))),
                                          how = "left") \
                                    .withColumn("constructor_key", xxhash64(col("constructor_id"))) \
                                    .select(
                                        col("constructor_key"),
                                        col("constructor_id"),
                                        col("name"),
                                        col("nationality"),
                                        col("started_time")
                                    )
        result_df = Gold_logic.add_unknown_row(result_df, "constructor_key")
        return result_df

    @staticmethod
    def transform_dim_engine_supplier(sources :dict) -> DataFrame:
        dim_engine_supplier_statsf1_df = sources["DIM_ENGINE_SUPPLIER_STATSF1"]

        result_df = dim_engine_supplier_statsf1_df.withColumn("engine_manufacturer_key", xxhash64(col("engine_manufacturer"))) \
                                                    .select(
                                                        col("engine_manufacturer_key"),
                                                        col("engine_manufacturer"),
                                                        col("nation"),
                                                        col("started_time")
                                                    )

        result_df = Gold_logic.add_unknown_row(result_df, "engine_manufacturer_key")

        return result_df

    @staticmethod
    def transform_fact_result(sources :dict) -> DataFrame:
        fact_result_df = sources["FACT_RESULT"]
        fact_result_statsf1_df = sources["FACT_RESULTS_STATSF1"]
        xwalk_driver_df = sources["XWALK_DRIVER"]
        xwalk_driver_df = sources["XWALK_DRIVER"].withColumnRenamed("master_key", "driver_id") \
                                                    .withColumnRenamed("source_native_key", "driver_name")


        result_df = fact_result_df.join(xwalk_driver_df, on = "driver_id", how = "left") \
                                    .join(fact_result_statsf1_df,(
                                        (fact_result_df.race_id == fact_result_statsf1_df.race_id) &
                                        (Gold_logic.normalize(xwalk_driver_df.driver_name) == Gold_logic.normalize(fact_result_statsf1_df.driver_name))),how = "left" ) \
                                    .withColumn("session_key", xxhash64(concat(fact_result_df.race_id,lit("_R")))) \
                                    .withColumn("driver_key", xxhash64(col("driver_id"))) \
                                    .withColumn("constructor_key", xxhash64(col("constructor_id"))) \
                                    .withColumn("engine_manufacturer_key", when(col("engine_manufacturer").isNull(), lit(-1)).otherwise(xxhash64(col("engine_manufacturer")))) \
                                    .select(
                                        col("session_key"),
                                        col("driver_key"),
                                        col("constructor_key"),
                                        col("engine_manufacturer_key"),
                                        col("chassis"),
                                        fact_result_df.position,
                                        col("grid"),
                                        col("laps"),
                                        col("is_completed_full_laps"),
                                        col("points"),
                                        col("total_race_time_ms"),
                                        col("gap_to_leader"),
                                        col("fastest_lap_rank"),
                                        col("fastest_lap_number"),
                                        col("fastest_lap_time"),
                                        col("speed").alias("average_speed"),
                                        col("units"),
                                        fact_result_df.status,
                                        col("description").alias("result_status_description"),
                                        col("season")
                                    )
        return result_df

    @staticmethod
    def transform_fact_lap(sources :dict) -> DataFrame:
        fact_lap_df = sources["FACT_LAP"]
        fact_result_df = sources["FACT_RESULT"]
        fact_result_df = fact_result_df.withColumn("session_id", concat(col("race_id"),lit("_R"))) \
                                        .withColumn("driver_key",
                                                    when(col("driver_id").isNull(), lit(-1)).otherwise(xxhash64(col("driver_id")))) \
                                        .withColumn("constructor_key",
                                                    when(col("constructor_id").isNull(), lit(-1)).otherwise(xxhash64(col("constructor_id"))))

        # handle null lap_start_time and lap_end_time by filling them with previous lap_end_time and next lap_start_time respectively, if available. If not available, fill with lap_end_time - lap_time for start and lap_start_time + lap_time for end.
        w = Window.partitionBy("session_id","driver_tla").orderBy("lap_number")
        fact_lap_df = fact_lap_df.withColumn("prev_end", lag("lap_end_time").over(w)) \
                                    .withColumn("next_start", lead("lap_start_time").over(w)) \
                                    .withColumn("start_filled",
                                                coalesce(col("lap_start_time"), col("prev_end"), col("lap_end_time") - col("lap_time"))) \
                                    .withColumn("end_filled",
                                                coalesce(col("lap_end_time"), col("next_start"), col("lap_start_time") + col("lap_time"))) \
                                    .withColumn("is_start_estimated", col("lap_start_time").isNull() & col("start_filled").isNotNull()) \
                                    .withColumn("is_end_estimated", col("lap_end_time").isNull() & col("end_filled").isNotNull()) \
                                    .drop("prev_end", "next_start")


        result_df = fact_lap_df.join(fact_result_df, on = ["session_id","permanent_number"], how = "left") \
                                .withColumn("session_key", xxhash64(col("session_id"))) \
                                .select(
                                    col("session_key"),
                                    col("driver_key"),
                                    col("constructor_key"),
                                    col("permanent_number"),
                                    col("lap_number"),
                                    col("lap_time"),
                                    col("sector_1_time"),
                                    col("sector_2_time"),
                                    col("sector_3_time"),
                                    col("start_filled").alias("lap_start_time"),
                                    col("is_start_estimated").alias("is_lap_start_time_estimated"),
                                    col("end_filled").alias("lap_end_time"),
                                    col("is_end_estimated").alias("is_lap_end_time_estimated"),
                                    col("stint"),
                                    col("compound"),
                                    col("tyre_life"),
                                    col("fresh_tyre"),
                                    col("pit_in_time"),
                                    col("pit_out_time"),
                                    col("track_status"),
                                    col("is_accurate"),
                                    fact_lap_df.season
                                )
        return result_df

    @staticmethod
    def transform_fact_telemetry(sources :dict) -> DataFrame:
        fact_telemetry_df = sources["FACT_TELEMETRY"]
        fact_result_df = sources["FACT_RESULT"]
        fact_lap_df = sources["FACT_LAP"]

        w = Window.partitionBy("session_key","permanent_number").orderBy("lap_number")
        fact_lap_df = fact_lap_df.withColumn("valid_to", coalesce(lead("lap_start_time").over(w), col("lap_end_time")))

        fact_result_df = fact_result_df.withColumn("session_id", concat(col("race_id"),lit("_R"))) \
                                                .withColumn("driver_key", when(col("driver_id").isNull(), lit(-1)).otherwise(xxhash64(col("driver_id"))))

        fact_telemetry_df = fact_telemetry_df.withColumn("session_key", xxhash64(col("session_id")))

        result_df = fact_telemetry_df.join(fact_result_df, on = ["session_id","permanent_number"], how = "left") \
                                        .join(broadcast(fact_lap_df.select("session_key","permanent_number","lap_number","lap_start_time","valid_to")), \
                                              ((fact_telemetry_df.session_key == fact_lap_df.session_key) &
                                               (fact_telemetry_df.permanent_number == fact_lap_df.permanent_number) &
                                               (fact_telemetry_df.session_time >= fact_lap_df.lap_start_time) &
                                               (fact_telemetry_df.session_time < fact_lap_df.valid_to)),
                                            how = "left") \
                                        .select(
                                            fact_telemetry_df.session_key,
                                            col("driver_key"),
                                            fact_telemetry_df.permanent_number,
                                            col("lap_number"),
                                            col("event_time"),
                                            col("session_time"),
                                            fact_telemetry_df.time.alias("lap_time"),
                                            fact_telemetry_df.speed,
                                            col("throttle"),
                                            col("brake"),
                                            col("rpm"),
                                            col("n_gear"),
                                            col("drs"),
                                            col("distance"),
                                            col("x"),
                                            col("y"),
                                            col("z"),
                                            fact_telemetry_df.season
                                        )
        return result_df

    @staticmethod
    def transform_fact_weather(sources :dict) -> DataFrame:
        fact_weather_df = sources["FACT_WEATHER"]

        result_df = fact_weather_df.withColumn("session_key", xxhash64(col("session_id"))) \
                                    .select(
                                        col("session_key"),
                                        col("time").alias("session_time"),
                                        col("air_temp"),
                                        col("track_temp"),
                                        col("humidity"),
                                        col("rain_fall"),
                                        col("wind_speed"),
                                        col("wind_direction"),
                                        col("pressure"),
                                        col("season")
                                    )

        return result_df
