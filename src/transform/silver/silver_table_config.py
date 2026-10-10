from dataclasses import dataclass
from typing import List, Optional, Dict, Callable
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from transform.silver.schema_migrator import sql, DerivedLogic
from enum import Enum
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType
)

from metadata.f1_topic import F1Topic
from metadata.IngestionMode import DataIngestionType
from transform.common.schemas import F1Schemas

class WriteStrategy(str, Enum):
    APPEND = "append"
    MERGE = "merge"
    DYNAMIC_OVERWRITE = "dynamic_overwrite"


    def __str__(self):
        return self.value

@dataclass(kw_only=True)
class Base:
    table_name : str
    ingestion_type : str
    primary_keys : List[str]
    write_strategy : str
    column_mapping : Optional[Dict[str,str]] = None
    partition_cols : Optional[List[str]] = None
    replace_condition_cols : Optional[List[str]] = None
    time_column : str = "kafka_timestamp"

@dataclass
class BaseIngestionConfig(Base):
    source_topic : str
    schema : StructType
    computed_columns : Optional[Dict[str,str]] = None
@dataclass
class DerivedTableConfig(Base):
    source_table : Dict[str,StructType]
    transformation_fn: Callable[..., DataFrame]
    silver_source_table : Optional[Dict[str,StructType]] = None

BASE_TABLE_CONFIGS = {
    #Config for base table
    #---------------------------------------------
    "FACT_TELEMETRY" : BaseIngestionConfig(
        table_name = "FACT_TELEMETRY",
        source_topic = F1Topic.FASTF1_TELEMETRY,
        ingestion_type = DataIngestionType.STREAMING_INGESTION,
        schema = F1Schemas.telemetry_schema,
        write_strategy = WriteStrategy.APPEND, #data will not change in the future
        column_mapping={
            "PermanentNumber" : "permanent_number",
            "Date" : "event_time",
            "SessionTime" : "session_time",
            "nGear" : "n_gear"
        },
        computed_columns={
            "time" : sql.deltaTime_to_float("time"),
            "session_time" : sql.deltaTime_to_float("session_time"),
            "permanent_number" : "cast(permanent_number as int)",
            "event_time" : "cast(event_time as timestamp)",
            "rpm" : "cast(rpm as int)",
            "n_gear" : "cast(n_gear as int)",
            "drs" : "cast(drs as int)"
        },
        primary_keys = ["session_id","permanent_number","event_time"],
        partition_cols = ["season"]
    ),
    "FACT_WEATHER" : BaseIngestionConfig(
        table_name = "FACT_WEATHER",
        source_topic = F1Topic.FASTF1_WEATHER,
        ingestion_type = DataIngestionType.STREAMING_INGESTION,
        schema = F1Schemas.weather_schema,
        write_strategy = WriteStrategy.APPEND,  #data will not change in the future
        column_mapping={
            "AirTemp" : "air_temp",
            "TrackTemp" : "track_temp",
            "Rainfall" : "rain_fall",
            "WindSpeed" : "wind_speed",
            "WindDirection" : "wind_direction"
        },
        computed_columns = {
            "time" : sql.deltaTime_to_float("time"),
            "wind_direction" : "cast(wind_direction as int)"
        }, 
        primary_keys = ["session_id","time"],
        partition_cols= ["season"]
    ),
    "FACT_LAP" : BaseIngestionConfig(
        table_name = "FACT_LAP",
        source_topic = F1Topic.FASTF1_LAP,
        ingestion_type = DataIngestionType.STREAMING_INGESTION,
        schema = F1Schemas.lap_schema,
        write_strategy = WriteStrategy.MERGE, #Data can be change, exp a lap can be cancel because of track limit ..., in the middle of the race
        column_mapping = {
            "Driver" : "driver_tla",
            "DriverNumber" : "permanent_number",
            "LapNumber" : "lap_number",
            "LapTime" : "lap_time",
            "LapStartTime" : "lap_start_time",
            "Time" : "lap_end_time",
            "TyreLife" : "tyre_life",
            "FreshTyre" : "fresh_tyre",
            "Sector1Time" : "sector_1_time",
            "Sector2Time" : "sector_2_time",
            "Sector3Time" : "sector_3_time",
            "PitInTime" : "pit_in_time",
            "PitOutTime" : "pit_out_time",
            "TrackStatus" : "track_status",
            "IsAccurate" : "is_accurate",
        },
        computed_columns = {
            "lap_time" : sql.deltaTime_to_float("lap_time"),
            "lap_start_time" : sql.deltaTime_to_float("lap_start_time"),
            "lap_end_time" : sql.deltaTime_to_float("lap_end_time"),
            "sector_1_time" : sql.deltaTime_to_float("sector_1_time"),
            "sector_2_time" : sql.deltaTime_to_float("sector_2_time"),
            "sector_3_time" : sql.deltaTime_to_float("sector_3_time"),
            "pit_in_time" : sql.deltaTime_to_float("pit_in_time"),
            "pit_out_time" : sql.deltaTime_to_float("pit_out_time"),
            "permanent_number" : "cast(permanent_number as int)",
            "tyre_life" : "cast(tyre_life as int)",
            "lap_number" : "cast(lap_number as int)",
            "stint": "cast(stint as int)",
        },
        primary_keys = ["session_id","driver_tla","lap_number"],
        partition_cols = ["season"]
    ),
    "DIM_DRIVER" : BaseIngestionConfig(
        table_name = "DIM_DRIVER",
        source_topic = F1Topic.JOLPICA_DRIVER,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.driver_schema,
        write_strategy = WriteStrategy.MERGE,
        column_mapping = {
            "driverId" : "driver_id",
            "permanentNumber" : "permanent_number",
            "code" : "driver_tla",
            "givenName" : "given_name",
            "familyName" : "family_name",
            "dateOfBirth" : "date_of_birth",
        },
        computed_columns = {
            "permanent_number" : "cast(permanent_number as int)"
        },
        primary_keys=["driver_id"]
    ),
    "DIM_CONSTRUCTOR" : BaseIngestionConfig(
        table_name = "DIM_CONSTRUCTOR",
        source_topic = F1Topic.JOLPICA_CONSTRUCTOR,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.constructor_schema,
        write_strategy = WriteStrategy.MERGE,
        column_mapping = {
            "constructorId" : "constructor_id"
        },
        primary_keys = ["constructor_id"]
    ),
    "DIM_DRIVER_STATSF1" : BaseIngestionConfig(
        table_name = "DIM_DRIVER_STATSF1",
        source_topic = F1Topic.STATSF1_DRIVER_STATSF1,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.driver_statsf1_schema,
        write_strategy = WriteStrategy.MERGE,
        column_mapping={
            "Driver" : "driver_name",
            "Constructor" : "constructor_name"
        },
        primary_keys=["driver_name","season","constructor_name"]
    ),
    "DIM_ENGINE_SUPPLIER_STATSF1" : BaseIngestionConfig(
        table_name = "DIM_ENGINE_SUPPLIER_STATSF1",
        source_topic = F1Topic.STATSF1_ENGINE_SUPPLIER_STATSF1,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.engine_supplier_statsf1_schema,
        write_strategy = WriteStrategy.MERGE,
        computed_columns = {
            "started_year" : "cast(started_time as int)"
        },
        primary_keys = ["engine_manufacturer"]
    ),
    "DIM_CAR_STATSF1" : BaseIngestionConfig(
        table_name = "DIM_CAR_STATSF1",
        source_topic = F1Topic.STATSF1_CAR_STATSF1,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.car_statsf1_schema,
        write_strategy = WriteStrategy.MERGE,
        column_mapping = {
            "Constructor" : "constructor_name",
        },
        primary_keys = ["constructor_name", "chassis", "engine"]
    ),
    "DIM_CONSTRUCTOR_STATSF1" : BaseIngestionConfig(
        table_name = "DIM_CONSTRUCTOR_STATSF1",
        source_topic = F1Topic.STATSF1_CONSTRUCTOR_STATSF1,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.constructor_statsf1_schema,
        write_strategy = WriteStrategy.MERGE,
        column_mapping = {
            "Constructor" : "constructor_name"
        },
        primary_keys = ["constructor_name"]
    )}

DERIVED_TABLE_CONFIGS = {
    #config for derived table
    #------------------------------------------
    "FACT_RESULTS_STATSF1" : DerivedTableConfig(
        table_name = "FACT_RESULTS_STATSF1",
        source_table = {F1Topic.STATSF1_RACE_RESULT : F1Schemas.result_statsf1_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        write_strategy = WriteStrategy.DYNAMIC_OVERWRITE,
        column_mapping = {
            "Driver_number" : "permanent_number",
            "Driver" : "driver_name"
        },
        replace_condition_cols = ["race_id"],
        transformation_fn = DerivedLogic.transform_dim_race_result_statsf1,
        primary_keys = ["race_id", "permanent_number", "position"],
    ),
    "DIM_CIRCUIT" : DerivedTableConfig(
        table_name = "DIM_CIRCUIT",
        source_table = {F1Topic.JOLPICA_RACE : F1Schemas.race_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        write_strategy = WriteStrategy.MERGE,
        column_mapping = {
            "circuitId" : "circuit_id",
            "circuitName" : "circuit_name"
        },
        transformation_fn = DerivedLogic.transform_dim_circuit,
        primary_keys = ["circuit_id"]
    ),
    "DIM_RACE" : DerivedTableConfig(
        table_name = "DIM_RACE",
        source_table = {F1Topic.JOLPICA_RACE : F1Schemas.race_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        write_strategy = WriteStrategy.DYNAMIC_OVERWRITE,
        replace_condition_cols = ["race_id"],
        column_mapping = {
            "circuitId" : "circuit_id",
            "raceName" : "race_name"
        },
        transformation_fn = DerivedLogic.transform_dim_race,
        primary_keys = ["race_id", "circuit_id"]
    ),
    "DIM_SESSION" : DerivedTableConfig(
        table_name = "DIM_SESSION",
        source_table = {F1Topic.JOLPICA_RACE : F1Schemas.race_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        write_strategy = WriteStrategy.DYNAMIC_OVERWRITE,
        replace_condition_cols = ["session_id"],
        transformation_fn = DerivedLogic.transform_dim_session,
        primary_keys = ["session_id", "race_id"]
    ),
    "FACT_RESULT" : DerivedTableConfig(
        table_name = "FACT_RESULT",
        source_table = {F1Topic.JOLPICA_RACE_RESULT : F1Schemas.result_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        write_strategy = WriteStrategy.DYNAMIC_OVERWRITE,
        replace_condition_cols = ["race_id"],
        column_mapping = {
            "number" : "permanent_number"
        },
        transformation_fn = DerivedLogic.transform_fact_result,
        primary_keys = ["race_id","driver_id","constructor_id"]
    ),
    "XWALK_CONSTRUCTOR": DerivedTableConfig(
        table_name = "XWALK_CONSTRUCTOR",
        source_table = {F1Topic.JOLPICA_CONSTRUCTOR : F1Schemas.constructor_schema,
                        F1Topic.STATSF1_CONSTRUCTOR_STATSF1 : F1Schemas.constructor_statsf1_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        write_strategy = WriteStrategy.MERGE,
        transformation_fn = DerivedLogic.transform_xwalk_constructor,
        primary_keys = ["source", "source_native_key"]
    ),
    "XWALK_DRIVER": DerivedTableConfig(
        table_name = "XWALK_DRIVER",
        source_table = {F1Topic.JOLPICA_RACE_RESULT : F1Schemas.result_schema,
                        F1Topic.STATSF1_DRIVER_STATSF1 : F1Schemas.driver_statsf1_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        write_strategy = WriteStrategy.MERGE,
        silver_source_table = {"XWALK_CONSTRUCTOR" : StructType([
            StructField("source", StringType(), True),
            StructField("source_native_key", StringType(), True),
            StructField("master_key", StringType(), True),
            StructField("confidence_score", FloatType(), True),
            StructField("silver_load_time", TimestampType(), True)
        ])},
        transformation_fn = DerivedLogic.transform_xwalk_driver,
        primary_keys = ["source", "source_native_key"]
    )
}



    