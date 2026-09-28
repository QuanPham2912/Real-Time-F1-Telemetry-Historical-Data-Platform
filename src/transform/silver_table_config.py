from dataclasses import dataclass
from typing import List, Optional, Dict, Callable
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from transform.schema_migrator import sql, DerivedLogic
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from metadata.f1_topic import F1Topic
from metadata.IngestionMode import DataIngestionType
from transform.schemas import F1Schemas

@dataclass
class BaseIngestionConfig:
    table_name : str
    source_topic : str
    ingestion_type : str
    schema : StructType
    primary_keys : List[str]
    column_mapping : Optional[Dict[str,str]] = None
    computed_columns : Optional[Dict[str,str]] = None
    partition_cols : Optional[List[str]] = None
    time_column : str = "kafka_timestamp"

@dataclass
class DerivedTableConfig:
    table_name : str
    source_table : Dict[str,StructType]
    ingestion_type : str
    transformation_fn: Callable[..., DataFrame]
    primary_keys : List[str]
    silver_source_table : Optional[Dict[str,StructType]] = None
    column_mapping : Optional[Dict[str,str]] = None
    partition_cols : Optional[List[str]] = None
    time_column : str = "kafka_timestamp"

BASE_TABLE_CONFIGS = {
    #Config for base table
    #---------------------------------------------
    "FACT_TELEMETRY" : BaseIngestionConfig(
        table_name = "FACT_TELEMETRY",
        source_topic = F1Topic.FASTF1_TELEMETRY,
        ingestion_type = DataIngestionType.STREAMING_INGESTION,
        schema = F1Schemas.telemetry_schema,
        column_mapping={
            "PermanentNumber" : "permanent_number",
            "Date" : "event_time",
            "SessionTime" : "session_time",
            "nGear" : "n_gear"
        },
        computed_columns={
            "time" : sql.deltaTime_to_float("time"),
            "session_time" : sql.deltaTime_to_float("session_time")
        },
        primary_keys = ["session_id","permanent_number","event_time"],
        partition_cols = ["season"]
    ),
    "FACT_WEATHER" : BaseIngestionConfig(
        table_name = "FACT_WEATHER",
        source_topic = F1Topic.FASTF1_WEATHER,
        ingestion_type = DataIngestionType.STREAMING_INGESTION,
        schema = F1Schemas.weather_schema,
        column_mapping={
            "AirTemp" : "air_temp",
            "TrackTemp" : "track_temp",
            "Rainfall" : "rain_fall",
            "WindSpeed" : "wind_speed",
            "WindDirection" : "wind_direction"
        },
        computed_columns = {
            "time" : sql.deltaTime_to_float("time")
        }, 
        primary_keys = ["session_id","time"],
        partition_cols= ["season"]
    ),
    "FACT_LAP" : BaseIngestionConfig(
        table_name = "FACT_LAP",
        source_topic = F1Topic.FASTF1_LAP,
        ingestion_type = DataIngestionType.STREAMING_INGESTION,
        schema = F1Schemas.lap_schema,
        column_mapping = {
            "Driver" : "driver_tla",
            "LapNumber" : "lap_number",
            "LapTime" : "lap_time",
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
            "sector_1_time" : sql.deltaTime_to_float("sector_1_time"),
            "sector_2_time" : sql.deltaTime_to_float("sector_2_time"),
            "sector_3_time" : sql.deltaTime_to_float("sector_3_time"),
            "pit_in_time" : sql.deltaTime_to_float("pit_in_time"),
            "pit_out_time" : sql.deltaTime_to_float("pit_out_time")
        },
        primary_keys = ["session_id","driver_tla","lap_number"],
        partition_cols = ["season"]
    ),
    "DIM_DRIVER" : BaseIngestionConfig(
        table_name = "DIM_DRIVER",
        source_topic = F1Topic.JOLPICA_DRIVER,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.driver_schema,
        column_mapping = {
            "driverId" : "driver_id",
            "permanentNumber" : "permanent_number",
            "code" : "driver_tla",
            "givenName" : "given_name",
            "familyName" : "family_name",
            "dateOfBirth" : "date_of_birth",
        },
        primary_keys=["driver_id"]
    ),
    "DIM_CONSTRUCTOR" : BaseIngestionConfig(
        table_name = "DIM_CONSTRUCTOR",
        source_topic = F1Topic.JOLPICA_CONSTRUCTOR,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.constructor_schema,
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
        primary_keys = ["engine_manufacturer"]
    ),
    "DIM_CAR_STATSF1" : BaseIngestionConfig(
        table_name = "DIM_CAR_STATSF1",
        source_topic = F1Topic.STATSF1_CAR_STATSF1,
        ingestion_type = DataIngestionType.BATCH_INGESTION,
        schema = F1Schemas.car_statsf1_schema,
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
        column_mapping = {
            "Driver_number" : "permanent_number",
            "Driver" : "driver_name"
        },
        transformation_fn = DerivedLogic.transform_dim_race_result_statsf1,
        primary_keys = ["race_id", "driver_number", "position"],
    ),
    "DIM_CIRCUIT" : DerivedTableConfig(
        table_name = "DIM_CIRCUIT",
        source_table = {F1Topic.JOLPICA_RACE : F1Schemas.race_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
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
        transformation_fn = DerivedLogic.transform_dim_session,
        primary_keys = ["session_id", "race_id"]
    ),
    "FACT_RESULT" : DerivedTableConfig(
        table_name = "FACT_RESULT",
        source_table = {F1Topic.JOLPICA_RACE_RESULT : F1Schemas.result_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
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
        transformation_fn = DerivedLogic.transform_xwalk_constructor,
        primary_keys = ["source", "source_native_key"]
    ),
    "XWALK_DRIVER": DerivedTableConfig(
        table_name = "XWALK_DRIVER",
        source_table = {F1Topic.JOLPICA_RACE_RESULT : F1Schemas.result_schema,
                        F1Topic.STATSF1_DRIVER_STATSF1 : F1Schemas.driver_statsf1_schema},
        ingestion_type = DataIngestionType.BATCH_INGESTION,
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



    