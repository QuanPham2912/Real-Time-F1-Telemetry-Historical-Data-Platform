from metadata.f1_topic import F1Topic
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower, trim, regexp_replace, pandas_udf, concat, concat_ws, to_timestamp, lit, create_map, coalesce, regexp_extract, col, when, min as _min, max as _max, expr
from pyspark.sql.window import Window
from pyspark.sql import Column
from metadata.f1_result_status import F1ResultStatus
from itertools import chain
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from rapidfuzz import fuzz, process
import pandas as pd

class sql:
    @staticmethod
    def deltaTime_to_float(col_name: str) -> str:
        return fr"""
        CAST(
            coalesce(cast(regexp_extract({col_name}, '(\\d+)\\s*days?', 1) as float), 0) * 86400 +
            
            cast(split(regexp_replace({col_name}, '.*days?\\s*', ''), ':')[0] as float) * 3600 +

            cast(split(regexp_replace({col_name}, '.*days?\\s*', ''), ':')[1] as float) * 60 +
            
            cast(split(regexp_replace({col_name}, '.*days?\\s*', ''), ':')[2] as float)
        AS FLOAT)
        """

class DerivedLogic:
    @staticmethod
    def transform_dim_race_result_statsf1(sources : dict):
        status_dict = F1ResultStatus.get_code_mapping()
        #Create pyspark mapping table
        mapping_expr = create_map([lit(x) for x in chain(*status_dict.items())])
        raw_df = sources[F1Topic.STATSF1_RACE_RESULT]
        session_window = Window.partitionBy("Race_id")
        df_with_max_laps = raw_df.withColumn("max_laps", _max("Total_lap").over(session_window))
        
        # implement logic to extract and type casting total time to finish the race from race time
        h = coalesce(regexp_extract(col("Race_time"), r"(\d+)h", 1).cast("double"), lit(0.0))
        m = coalesce(regexp_extract(col("Race_time"), r"(\d+)m", 1).cast("double"), lit(0.0))
        s = coalesce(regexp_extract(col("Race_time"), r"([\d\.]+)s", 1).cast("double"), lit(0.0))
        race_time_ms = ((h * 3600) + (m * 60) + s) * 1000

        result_df = df_with_max_laps.select(
            col("Race_id"),
            when(col("Position").rlike("^[0-9]+$"), col("Position").cast("integer")).otherwise(None).alias("Position"),
            col("Driver_number"),
            col("Driver"),
            col("Chassis"),
            col("Engine_manufacturer"),
            col("Total_lap"),
            when(col("Total_lap") == col("max_laps"), True).otherwise(False).alias("is_completed_full_laps"),
            when(col("Total_lap") == col("max_laps"),race_time_ms).otherwise(None).alias("total_race_time_ms"),
            when((col("Total_lap") == col("max_laps")) & (col("Position") != "1"),  regexp_extract(col("Race_time"), r"\(\s*(\+[^)]+)\)", 1)).otherwise(None).alias("gap_to_leader"),
            when(col("Position").rlike("^[0-9]+$"), F1ResultStatus.FINISHED).otherwise(
                when(mapping_expr[lower(col("Position"))].isNotNull(), mapping_expr[lower(col("Position"))]).otherwise(expr("upper(Position)"))
            ).alias("Status"),
            when((~col("Position").rlike("^[0-9]+$")) & (col("Race_time").isNotNull()), col("Race_time")).otherwise(None).alias("Description")
        )

        return result_df

    @staticmethod
    def transform_dim_circuit(sources: dict):
        raw_df = sources[F1Topic.JOLPICA_RACE]
        result_df = raw_df.select(
            col("Circuit.circuitId"),
            col("Circuit.circuitName"),
            col("Circuit.Location.*"),
            col("source")
        )

        return result_df

    @staticmethod
    def transform_dim_race(sources: dict):
        raw_df = sources[F1Topic.JOLPICA_RACE]
        result_df = raw_df.select(
            col("race_id"),
            col("season"),
            col("round"),
            col("Circuit.circuitId"),
            col("raceName"),
            to_timestamp(
                concat_ws("T", col("date"), col("time")),
                "yyyy-MM-dd'T'HH:mm:ss'Z'",
            ).alias("date"),
            col("source")
        )

        return result_df

    @staticmethod
    def transform_dim_session(sources: dict):
        raw_df = sources[F1Topic.JOLPICA_RACE]
        result_df = raw_df.select(
            #In the future if adding more session type to project this function have to edited as well
            concat(col("race_id"), lit("R")).alias("session_id"),
            col("race_id"),
            lit("R").alias("session_type"),
            # if adding more session type the data need to concern to that type of session
            to_timestamp(concat_ws("T",  col("date"), col("time")),"yyyy-MM-dd'T'HH:mm:ss'Z'").alias("date"),
            col("source")
        )

        return result_df

    @staticmethod
    def transform_fact_result(sources: dict):
        raw_df = sources[F1Topic.JOLPICA_RACE_RESULT]
        result_df = raw_df.select(
            col("race_id"),
            col("Driver.driverId").alias("driver_id"),
            col("Constructor.constructorId").alias("constructor_id"),
            col("number"),
            col("position"),
            col("points"),
            col("grid"),
            col("laps"),
            col("status"),
            col("Time.*"),
            col("FastestLap.rank"),
            col("FastestLap.lap"),
            col("FastestLap.Time.time").alias("fastest_time"),
            col("FastestLap.AverageSpeed.*"),
            col("source")
        )

        return result_df

    #-----------------------------------------------------------------------
    @staticmethod
    def transform_xwalk_constructor(sources :dict):
        dim_constructor = sources[F1Topic.JOLPICA_CONSTRUCTOR]
        dim_constructor_statsf1 = sources[F1Topic.STATSF1_CONSTRUCTOR_STATSF1]
        dim_rows = dim_constructor.select("constructorId", "name", "nationality").collect()
        dim_map_name = {row.constructorId: row.name for row in dim_rows}
        dim_map_nation = {row.constructorId: row.nationality for row in dim_rows}
        
        @pandas_udf(StructType([
            StructField("master_key", StringType(), True),
            StructField("confidence_score", FloatType(), True)
        ]))
        def match_constructor_udf(statsf1_name :pd.Series, statsf1_nation :pd.Series):
            results = []
            for name, nation in zip(statsf1_name, statsf1_nation):
                if not name or pd.isna(name):
                    results.append((None, 0.0))
                    continue
                
                candidates = process.extract(name, dim_map_name, scorer=fuzz.WRatio, limit = 5)
                if not candidates:
                    results.append((None,0.0))
                    continue
                best_match_name, best_confidence_score, constructor_id = candidates[0]
                if len(candidates) > 1 and (best_confidence_score - candidates[1][1]) < 5:
                    same_nation = [c for c in candidates  if dim_map_nation.get(c[2]) and nation and dim_map_nation.get(c[2]).casefold() == nation.casefold()]
                    if(len(same_nation) == 1):
                        best_match_name, best_confidence_score, constructor_id = same_nation[0]

                results.append((constructor_id, float(best_confidence_score/100)))

            return pd.DataFrame(results, columns=["master_key","confidence_score"])

        matched_df = dim_constructor_statsf1.withColumn(
            "match_result",
            match_constructor_udf(col("Constructor"), col("Nation"))
        )

        return matched_df.select(
            col("source"),
            col("Constructor").alias("source_native_key"),
            col("match_result.*")
        )


    #Must run after xwalk constructor finished
    @staticmethod
    def transform_xwalk_driver(sources :dict):
        fact_result = sources[F1Topic.JOLPICA_RACE_RESULT]
        dim_driver_statsf1 = sources[F1Topic.STATSF1_DRIVER_STATSF1]
        xwalk_constructor = sources["XWALK_CONSTRUCTOR"]
        def normalize(column: Column) -> Column:
                return trim(regexp_replace(lower(column), r"\s+", " "))

        #Use the xwalk constructor to convert from constructor name to constructor ID.
        statsf1_driver = dim_driver_statsf1.alias("statsf1_driver")
        statsf1_constructor_xwalk = xwalk_constructor.filter(
            lower(col("source")) == "stats_f1"
        ).alias("statsf1_constructor_xwalk")
        dim_driver_statsf1 = statsf1_driver.join(
            statsf1_constructor_xwalk,
            normalize(col("statsf1_driver.Constructor"))
            == normalize(col("statsf1_constructor_xwalk.source_native_key")),
            "left"
        ).select(
            col("statsf1_driver.Driver").alias("Driver"),
            col("statsf1_driver.Season").alias("Season"),
            col("statsf1_constructor_xwalk.master_key").alias("constructor_id"),
            col("statsf1_driver.Source").alias("Source"),
        )

        dim_rows = fact_result.select(
            col("Driver.driverId"),
            concat(col("Driver.familyName"),lit(" "),col("Driver.givenName")).alias("driver_name"),
            col("Constructor.constructorId"),
            col("season")
        ).distinct().collect()

        dim_map_name = {row.driverId: row.driver_name for row in dim_rows}
        dim_map_constructor_season = {}
        for row in dim_rows:
            dim_map_constructor_season.setdefault(row.driverId, set()).add(
                (row.constructorId, row.season)
            )
        

        @pandas_udf(StructType([
            StructField("master_key", StringType(), True),
            StructField("confidence_score", FloatType(), True)
        ]))
        def match_driver_udf(statsf1_name :pd.Series, statsf1_constructor_id :pd.Series, statsf1_season :pd.Series):
            results = []
            for name, constructor_id , season in zip(statsf1_name, statsf1_constructor_id, statsf1_season):
                if not name or pd.isna(name):
                    results.append((None, 0.0))
                    continue

                candidates = process.extract(name, dim_map_name, scorer = fuzz.WRatio, limit = 5)
                if not candidates:
                    results.append((None,0.0))
                    continue
                best_match_name, best_confidence_score, driver_id = candidates[0]

                if len(candidates) > 1 and (best_confidence_score - candidates[1][1] < 5):
                    same_constructor_season = [
                        candidate
                        for candidate in candidates
                        if (constructor_id, season)
                        in dim_map_constructor_season.get(candidate[2], set())
                    ]
                    if len(same_constructor_season) == 1:
                        best_match_name, best_confidence_score, driver_id = same_constructor_season[0]

                results.append((driver_id, float(best_confidence_score/100)))
            return pd.DataFrame(results, columns=["master_key","confidence_score"])
        
        matched_df = dim_driver_statsf1.withColumn(
            "match_result",
            match_driver_udf(col("Driver"), col("constructor_id"), col("Season"))
        )

        return matched_df.select(
            col("Source"),
            col("Driver").alias("source_native_key"),
            col("match_result.*")
        )










    


