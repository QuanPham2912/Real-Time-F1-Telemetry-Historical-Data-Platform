import pytest

from transform.gold.gold_logic import Gold_logic
from transform.gold.gold_table_config import GOLD_TABLE_CONFIGS, GoldTableConfig


EXPECTED_CONFIGS = {
	"DIM_SESSION": {
		"silver_sources": ["DIM_SESSION", "DIM_RACE", "DIM_CIRCUIT"],
		"gold_sources": None,
		"transformation_fn": Gold_logic.transform_dim_session,
		"primary_keys": ["session_key"],
		"partition_cols": None,
	},
	"DIM_DRIVER": {
		"silver_sources": ["DIM_DRIVER"],
		"gold_sources": None,
		"transformation_fn": Gold_logic.transform_dim_driver,
		"primary_keys": ["driver_key"],
		"partition_cols": None,
	},
	"DIM_CONSTRUCTOR": {
		"silver_sources": [
			"DIM_CONSTRUCTOR",
			"XWALK_CONSTRUCTOR",
			"DIM_CONSTRUCTOR_STATSF1",
		],
		"gold_sources": None,
		"transformation_fn": Gold_logic.transform_dim_constructor,
		"primary_keys": ["constructor_key"],
		"partition_cols": None,
	},
	"DIM_ENGINE_SUPPLIER": {
		"silver_sources": ["DIM_ENGINE_SUPPLIER_STATSF1"],
		"gold_sources": None,
		"transformation_fn": Gold_logic.transform_dim_engine_supplier,
		"primary_keys": ["engine_manufacturer_key"],
		"partition_cols": None,
	},
	"FACT_RESULT": {
		"silver_sources": ["FACT_RESULT", "FACT_RESULTS_STATSF1", "XWALK_DRIVER"],
		"gold_sources": None,
		"transformation_fn": Gold_logic.transform_fact_result,
		"primary_keys": ["session_key", "driver_key", "constructor_key"],
		"partition_cols": ["season"],
	},
	"FACT_LAP": {
		"silver_sources": ["FACT_LAP", "FACT_RESULT"],
		"gold_sources": None,
		"transformation_fn": Gold_logic.transform_fact_lap,
		"primary_keys": ["session_key", "permanent_number", "lap_number"],
		"partition_cols": ["season"],
	},
	"FACT_TELEMETRY": {
		"silver_sources": ["FACT_TELEMETRY", "FACT_RESULT"],
		"gold_sources": ["FACT_LAP"],
		"transformation_fn": Gold_logic.transform_fact_telemetry,
		"primary_keys": [
			"session_key",
			"permanent_number",
			"event_time",
			"session_time",
		],
		"partition_cols": ["season"],
	},
	"FACT_WEATHER": {
		"silver_sources": ["FACT_WEATHER"],
		"gold_sources": None,
		"transformation_fn": Gold_logic.transform_fact_weather,
		"primary_keys": ["session_key", "session_time"],
		"partition_cols": ["season"],
	},
}


@pytest.mark.parametrize(
	("table_name", "expected"),
	EXPECTED_CONFIGS.items(),
	ids=EXPECTED_CONFIGS,
)
def test_gold_table_config_matches_expected_definition(table_name, expected):
	config = GOLD_TABLE_CONFIGS[table_name]

	assert isinstance(config, GoldTableConfig)
	assert config.table_name == table_name
	assert config.silver_sources == expected["silver_sources"]
	assert config.gold_sources == expected["gold_sources"]
	assert config.write_mode == "overwrite"
	assert config.transformation_fn is expected["transformation_fn"]
	assert config.primary_keys == expected["primary_keys"]
	assert config.partition_cols == expected["partition_cols"]


def test_gold_table_configs_contain_exactly_the_expected_tables():
	assert set(GOLD_TABLE_CONFIGS) == set(EXPECTED_CONFIGS)


def test_gold_table_config_optional_sources_and_partitions_default_to_none():
	config = GoldTableConfig(
		table_name="TEST_TABLE",
		silver_sources=["SILVER_SOURCE"],
		write_mode="overwrite",
		transformation_fn=Gold_logic.transform_dim_driver,
		primary_keys=["driver_key"],
	)

	assert config.gold_sources is None
	assert config.partition_cols is None