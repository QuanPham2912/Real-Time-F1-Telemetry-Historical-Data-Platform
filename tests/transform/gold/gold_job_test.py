import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, call

delta_package = ModuleType("delta")
delta_package.__path__ = []
delta_tables = ModuleType("delta.tables")
delta_tables.DeltaTable = Mock()
_missing_module = object()
_previous_delta = sys.modules.get("delta", _missing_module)
_previous_delta_tables = sys.modules.get("delta.tables", _missing_module)
sys.modules["delta"] = delta_package
sys.modules["delta.tables"] = delta_tables
try:
	gold_job_module = importlib.import_module("transform.gold.gold_job")
	GoldTableConfig = importlib.import_module(
		"transform.gold.gold_table_config"
	).GoldTableConfig
finally:
	for module_name, previous_module in (
		("delta", _previous_delta),
		("delta.tables", _previous_delta_tables),
	):
		if previous_module is _missing_module:
			sys.modules.pop(module_name, None)
		else:
			sys.modules[module_name] = previous_module


def build_delta_reader(dataframe):
	reader = Mock()
	reader.format.return_value = reader
	reader.load.return_value = dataframe
	return reader


def test_read_silver_source_loads_delta_from_silver_base_path():
	dataframe = Mock(name="silver_dataframe")
	reader = build_delta_reader(dataframe)
	job = gold_job_module.goldJob(
		SimpleNamespace(read=reader),
		silverBasePath="s3a://silver-root/",
	)

	result = job.read_silver_source("DIM_DRIVER")

	assert result is dataframe
	assert reader.method_calls == [
		call.format("delta"),
		call.load("s3a://silver-root/DIM_DRIVER"),
	]


def test_read_gold_source_loads_delta_from_gold_base_path():
	dataframe = Mock(name="gold_dataframe")
	reader = build_delta_reader(dataframe)
	job = gold_job_module.goldJob(
		SimpleNamespace(read=reader),
		goldBasePath="s3a://gold-root/",
	)

	result = job.read_gold_source("FACT_LAP")

	assert result is dataframe
	assert reader.method_calls == [
		call.format("delta"),
		call.load("s3a://gold-root/FACT_LAP"),
	]


def test_process_gold_table_reads_sources_deduplicates_and_writes(monkeypatch):
	silver_dataframes = {
		"DIM_SESSION": Mock(name="silver_session"),
		"DIM_RACE": Mock(name="silver_race"),
	}
	gold_dataframes = {"FACT_LAP": Mock(name="gold_lap")}
	transformed_dataframe = Mock(name="transformed_dataframe")
	deduplicated_dataframe = Mock(name="deduplicated_dataframe")
	transformed_dataframe.dropDuplicates.return_value = deduplicated_dataframe
	transformation = Mock(return_value=transformed_dataframe)
	config = GoldTableConfig(
		table_name="FACT_TELEMETRY",
		silver_sources=["DIM_SESSION", "DIM_RACE"],
		gold_sources=["FACT_LAP"],
		write_mode="overwrite",
		transformation_fn=transformation,
		primary_keys=["session_key", "permanent_number", "event_time"],
		partition_cols=["season"],
	)
	job = gold_job_module.goldJob(
		SimpleNamespace(),
		goldBasePath="s3a://gold-root/",
	)
	job.read_silver_source = Mock(
		side_effect=lambda *, silver_source: silver_dataframes[silver_source]
	)
	job.read_gold_source = Mock(
		side_effect=lambda *, gold_source: gold_dataframes[gold_source]
	)
	write_to_delta = Mock()
	monkeypatch.setattr(
		gold_job_module.DeltaBatchWriter,
		"write_to_delta",
		write_to_delta,
	)

	result = job.process_gold_table(config)

	assert result is None
	assert job.read_silver_source.call_args_list == [
		call(silver_source="DIM_SESSION"),
		call(silver_source="DIM_RACE"),
	]
	job.read_gold_source.assert_called_once_with(gold_source="FACT_LAP")
	transformation.assert_called_once_with(
		{
			"DIM_SESSION": silver_dataframes["DIM_SESSION"],
			"DIM_RACE": silver_dataframes["DIM_RACE"],
			"FACT_LAP": gold_dataframes["FACT_LAP"],
		}
	)
	transformed_dataframe.dropDuplicates.assert_called_once_with(
		config.primary_keys
	)
	write_to_delta.assert_called_once_with(
		df=deduplicated_dataframe,
		deltaPath="s3a://gold-root/FACT_TELEMETRY",
		outputMode="overwrite",
		partitionCols=["season"],
	)