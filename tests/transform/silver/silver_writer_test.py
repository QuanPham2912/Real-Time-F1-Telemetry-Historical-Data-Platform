import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

delta_package = ModuleType("delta")
delta_package.__path__ = []
delta_tables = ModuleType("delta.tables")
delta_tables.DeltaTable = Mock()
with patch.dict(sys.modules, {"delta": delta_package, "delta.tables": delta_tables}):
	silver_writer_module = importlib.import_module("transform.silver.silver_writer")


def test_silver_writer_uses_configured_path_and_forwards_writer_options(monkeypatch):
	monkeypatch.setattr(
		silver_writer_module,
		"settings",
		SimpleNamespace(silver_base_path="s3a://custom-silver/root/"),
	)
	writer = Mock()
	get_writer = Mock(return_value=writer)
	monkeypatch.setattr(silver_writer_module, "get_writer", get_writer)
	dataframe = Mock(name="dataframe")
	spark = Mock(name="spark")
	config = SimpleNamespace(
		table_name="DIM_RACE",
		ingestion_type="batch_ingestion",
		write_strategy="dynamic_overwrite",
		partition_cols=["season"],
		primary_keys=["race_id"],
		replace_condition_cols=["race_id"],
	)
	write_result = object()
	writer.write.return_value = write_result

	result = silver_writer_module.DeltaWriterEngine.silver_writer(
		dataframe, spark, config
	)

	assert result is write_result
	get_writer.assert_called_once_with("batch_ingestion", "dynamic_overwrite")
	writer.write.assert_called_once_with(
		df=dataframe,
		deltaPath="s3a://custom-silver/root/DIM_RACE",
		checkPointPath="s3a://custom-silver/root/checkpoints/DIM_RACE",
		partitionCols=["season"],
		primaryKeys=["race_id"],
		replaceConditionCols=["race_id"],
		sparkSession=spark,
	)


def test_silver_writer_uses_configured_base_path_and_passes_optional_values(monkeypatch):
	monkeypatch.setattr(
		silver_writer_module,
		"settings",
		SimpleNamespace(silver_base_path="s3a://f1-silver/"),
	)
	writer = Mock()
	monkeypatch.setattr(
		silver_writer_module, "get_writer", Mock(return_value=writer)
	)
	dataframe = Mock(name="dataframe")
	spark = Mock(name="spark")
	config = SimpleNamespace(
		table_name="FACT_TELEMETRY",
		ingestion_type="streaming_ingestion",
		write_strategy="append",
		partition_cols=None,
		primary_keys=["session_id", "event_time"],
		replace_condition_cols=None,
	)

	silver_writer_module.DeltaWriterEngine.silver_writer(dataframe, spark, config)

	writer.write.assert_called_once_with(
		df=dataframe,
		deltaPath="s3a://f1-silver/FACT_TELEMETRY",
		checkPointPath="s3a://f1-silver/checkpoints/FACT_TELEMETRY",
		partitionCols=None,
		primaryKeys=["session_id", "event_time"],
		replaceConditionCols=None,
		sparkSession=spark,
	)