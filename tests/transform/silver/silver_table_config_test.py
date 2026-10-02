import pytest
from pyspark.sql.types import StructType

from metadata.f1_topic import F1Topic
from metadata.IngestionMode import DataIngestionType
from transform.silver.schema_migrator import DerivedLogic
from transform.silver.silver_table_config import (
	BASE_TABLE_CONFIGS,
	DERIVED_TABLE_CONFIGS,
	BaseIngestionConfig,
	DerivedTableConfig,
	WriteStrategy,
)


@pytest.mark.parametrize(
	("config_name", "config"),
	list(BASE_TABLE_CONFIGS.items()),
	ids=list(BASE_TABLE_CONFIGS),
)
def test_base_table_configs_are_consistent(config_name, config):
	assert isinstance(config, BaseIngestionConfig)
	assert config.table_name == config_name
	assert isinstance(config.source_topic, F1Topic)
	assert isinstance(config.schema, StructType)
	assert config.ingestion_type in (
		DataIngestionType.BATCH_INGESTION,
		DataIngestionType.STREAMING_INGESTION,
	)
	assert config.primary_keys

	schema_columns = {field.name for field in config.schema.fields}
	assert set(config.column_mapping or {}).issubset(schema_columns)

	output_columns = {
		(config.column_mapping or {}).get(column, column).lower()
		for column in schema_columns
	}
	output_columns.update((config.computed_columns or {}).keys())
	assert set(config.primary_keys).issubset(output_columns)


@pytest.mark.parametrize(
	("config_name", "config"),
	list(DERIVED_TABLE_CONFIGS.items()),
	ids=list(DERIVED_TABLE_CONFIGS),
)
def test_derived_table_configs_are_consistent(config_name, config):
	assert isinstance(config, DerivedTableConfig)
	assert config.table_name == config_name
	assert config.ingestion_type in (
		DataIngestionType.BATCH_INGESTION,
		DataIngestionType.STREAMING_INGESTION,
	)
	assert config.source_table
	assert all(isinstance(schema, StructType) for schema in config.source_table.values())
	assert callable(config.transformation_fn)
	assert config.primary_keys
	assert all(isinstance(schema, StructType) for schema in (config.silver_source_table or {}).values())


def test_derived_table_configs_use_the_expected_transformations():
	expected_transformations = {
		"FACT_RESULTS_STATSF1": DerivedLogic.transform_dim_race_result_statsf1,
		"DIM_CIRCUIT": DerivedLogic.transform_dim_circuit,
		"DIM_RACE": DerivedLogic.transform_dim_race,
		"DIM_SESSION": DerivedLogic.transform_dim_session,
		"FACT_RESULT": DerivedLogic.transform_fact_result,
		"XWALK_CONSTRUCTOR": DerivedLogic.transform_xwalk_constructor,
		"XWALK_DRIVER": DerivedLogic.transform_xwalk_driver,
	}

	assert {
		name: config.transformation_fn for name, config in DERIVED_TABLE_CONFIGS.items()
	} == expected_transformations


def test_xwalk_driver_declares_constructor_crosswalk_source():
	config = DERIVED_TABLE_CONFIGS["XWALK_DRIVER"]

	assert set(config.silver_source_table) == {"XWALK_CONSTRUCTOR"}
	assert {field.name for field in config.silver_source_table["XWALK_CONSTRUCTOR"].fields} == {
		"source",
		"source_native_key",
		"master_key",
		"confidence_score",
		"silver_load_time",
	}


def test_dynamic_overwrite_configs_declare_replace_condition_columns():
	expected_replace_columns = {
		"FACT_RESULTS_STATSF1": ["race_id"],
		"DIM_RACE": ["race_id"],
		"DIM_SESSION": ["session_id"],
		"FACT_RESULT": ["race_id"],
	}
	actual_replace_columns = {
		name: config.replace_condition_cols
		for name, config in DERIVED_TABLE_CONFIGS.items()
		if config.write_strategy == WriteStrategy.DYNAMIC_OVERWRITE
	}

	assert actual_replace_columns == expected_replace_columns


def test_ingestion_config_dataclasses_have_optional_defaults():
	base_config = BaseIngestionConfig(
		table_name="table",
		source_topic=F1Topic.FASTF1_LAP,
		ingestion_type=DataIngestionType.BATCH_INGESTION,
		write_strategy=WriteStrategy.MERGE,
		schema=StructType([]),
		primary_keys=["id"],
	)
	derived_config = DerivedTableConfig(
		table_name="derived",
		source_table={},
		ingestion_type=DataIngestionType.BATCH_INGESTION,
		write_strategy=WriteStrategy.MERGE,
		transformation_fn=lambda sources: None,
		primary_keys=["id"],
	)

	for config in (base_config, derived_config):
		assert config.column_mapping is None
		assert config.partition_cols is None
		assert config.replace_condition_cols is None
		assert config.time_column == "kafka_timestamp"

	assert base_config.computed_columns is None
	assert derived_config.silver_source_table is None
