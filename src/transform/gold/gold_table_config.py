from dataclasses import dataclass
from typing import List, Optional, Callable
from pyspark.sql import DataFrame
from transform.gold.gold_logic import Gold_logic

@dataclass
class GoldTableConfig:
    table_name : str
    silver_sources : List[str]
    write_mode : str
    transformation_fn : Callable[..., DataFrame]
    primary_keys : List[str]
    gold_sources : Optional[List[str]] = None
    partition_cols : Optional[List[str]] = None

GOLD_TABLE_CONFIGS = {
    ### DIM TABLE
    "DIM_SESSION" : GoldTableConfig(
        table_name = "DIM_SESSION",
        silver_sources = ["DIM_SESSION","DIM_RACE","DIM_CIRCUIT"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_dim_session,
        primary_keys = ["session_key"]
        
    ),
    "DIM_DRIVER" : GoldTableConfig(
        table_name = "DIM_DRIVER",
        silver_sources = ["DIM_DRIVER"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_dim_driver,
        primary_keys = ["driver_key"]
    ),
    "DIM_CONSTRUCTOR" : GoldTableConfig(
        table_name = "DIM_CONSTRUCTOR",
        silver_sources = ["DIM_CONSTRUCTOR","XWALK_CONSTRUCTOR","DIM_CONSTRUCTOR_STATSF1"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_dim_constructor,
        primary_keys = ["constructor_key"]
    ),
    "DIM_ENGINE_SUPPLIER" : GoldTableConfig(
        table_name = "DIM_ENGINE_SUPPLIER",
        silver_sources = ["DIM_ENGINE_SUPPLIER_STATSF1"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_dim_engine_supplier,
        primary_keys = ["engine_manufacturer_key"]
    ),
    "FACT_RESULT" : GoldTableConfig(
        table_name = "FACT_RESULT",
        silver_sources = ["FACT_RESULT","FACT_RESULTS_STATSF1","XWALK_DRIVER"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_fact_result,
        primary_keys = ["session_key","driver_key","constructor_key"],
        partition_cols = ["season"]
    ),
    "FACT_LAP" : GoldTableConfig(
        table_name = "FACT_LAP",
        silver_sources = ["FACT_LAP","FACT_RESULT"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_fact_lap,
        primary_keys = ["session_key","permanent_number","lap_number"],
        partition_cols = ["season"]
    ),
    #Have to wait until FACT_LAP done
    "FACT_TELEMETRY" : GoldTableConfig(
        table_name = "FACT_TELEMETRY",
        silver_sources = ["FACT_TELEMETRY","FACT_RESULT"],
        gold_sources = ["FACT_LAP"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_fact_telemetry,
        primary_keys = ["session_key","permanent_number","event_time", "session_time"],
        partition_cols = ["season"]
    ),
    "FACT_WEATHER" : GoldTableConfig(
        table_name = "FACT_WEATHER",
        silver_sources = ["FACT_WEATHER"],
        write_mode = "overwrite",
        transformation_fn = Gold_logic.transform_fact_weather,
        primary_keys = ["session_key","session_time"],
        partition_cols = ["season"]
    ),
    
}