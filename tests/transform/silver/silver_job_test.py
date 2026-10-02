import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

delta_package = ModuleType("delta")
delta_package.__path__ = []
delta_tables = ModuleType("delta.tables")
delta_tables.DeltaTable = Mock()
with patch.dict(sys.modules, {"delta": delta_package, "delta.tables": delta_tables}):
	silver_job_module = importlib.import_module("transform.silver.silver_job")
	silverJob = silver_job_module.silverJob
	DataIngestionType = importlib.import_module(
		"metadata.IngestionMode"
	).DataIngestionType
	silver_table_config = importlib.import_module("transform.silver.silver_table_config")
	DerivedTableConfig = silver_table_config.DerivedTableConfig
	WriteStrategy = silver_table_config.WriteStrategy


@pytest.mark.parametrize(
	("ingestion_type", "reader_name"),
	[
		(DataIngestionType.BATCH_INGESTION, "read"),
		(DataIngestionType.STREAMING_INGESTION, "readStream"),
	],
)
def test_read_bronze_source_selects_reader_and_flattens_metadata(
	monkeypatch, ingestion_type, reader_name
):
	schema = StructType([StructField("value", StringType(), True)])
	bronze_df = Mock(name="bronze_df")
	parsed_df = Mock(name="parsed_df")
	flattened_df = Mock(name="flattened_df")
	reader = Mock()
	reader.format.return_value.load.return_value = bronze_df
	spark = SimpleNamespace(**{reader_name: reader})
	bronze_df.withColumn.return_value = parsed_df
	parsed_df.select.return_value = flattened_df
	from_json = Mock(return_value="parsed-json")
	columns = {}

	def make_column(name):
		column = Mock(name=name)
		column.alias.return_value = f"alias:{name}"
		columns[name] = column
		return column

	col = Mock(side_effect=make_column)
	timestamp_column = make_column("now")
	current_timestamp = Mock(return_value=timestamp_column)
	monkeypatch.setattr(silver_job_module, "from_json", from_json)
	monkeypatch.setattr(silver_job_module, "col", col)
	monkeypatch.setattr(silver_job_module, "current_timestamp", current_timestamp)

	job = silverJob(spark, bronzeBasePath="s3a://bronze/")
	result = job.read_bronze_source("topic-a", ingestion_type, schema)

	assert result is flattened_df
	reader.format.assert_called_once_with("delta")
	reader.format.return_value.load.assert_called_once_with("s3a://bronze/topic-a")
	from_json.assert_called_once_with(columns["kafka_value"], schema)
	bronze_df.withColumn.assert_called_once_with("data", "parsed-json")
	parsed_df.select.assert_called_once()


def test_apply_transformation_renames_configured_columns_then_lowercases():
	config = SimpleNamespace(column_mapping={"Driver": "driver_name", "missing": "ignored"})
	dataframe = Mock()
	dataframe.columns = ["Driver", "Season", "unchanged"]
	renamed_dataframe = Mock()
	renamed_dataframe.columns = ["driver_name", "Season", "unchanged"]
	dataframe.withColumnRenamed.return_value = renamed_dataframe
	renamed_dataframe.toDF.return_value = "normalized-dataframe"

	result = silverJob(Mock()).apply_transformation(dataframe, config)

	assert result == "normalized-dataframe"
	dataframe.withColumnRenamed.assert_called_once_with("Driver", "driver_name")
	renamed_dataframe.toDF.assert_called_once_with("driver_name", "season", "unchanged")


def test_process_derived_table_reads_silver_source_but_writes_output_table(monkeypatch):
	schema = StructType([StructField("master_key", StringType(), True)])
	source_df = Mock(name="source_df")
	output_df = Mock(name="output_df")
	output_df.dropna.return_value = output_df
	output_df.dropDuplicates.return_value = output_df
	transform = Mock(return_value=output_df)
	config = DerivedTableConfig(
		table_name="XWALK_DRIVER",
		source_table={},
		silver_source_table={"XWALK_CONSTRUCTOR": schema},
		ingestion_type=DataIngestionType.BATCH_INGESTION,
		write_strategy=WriteStrategy.MERGE,
		transformation_fn=transform,
		primary_keys=["source", "source_native_key"],
	)
	read = Mock()
	read.format.return_value.load.return_value = source_df
	spark = SimpleNamespace(read=read)
	writer = Mock()
	monkeypatch.setattr(silver_job_module.DeltaWriterEngine, "silver_writer", writer)
	job = silverJob(spark, silverBasePath="s3a://silver/")
	job.apply_transformation = Mock(return_value=output_df)
	writer.return_value = "writer-result"

	result = job.process_derived_table(config)

	assert result == "writer-result"
	read.format.assert_called_once_with("delta")
	read.format.return_value.load.assert_called_once_with("s3a://silver/XWALK_CONSTRUCTOR")
	transform.assert_called_once_with({"XWALK_CONSTRUCTOR": source_df})
	job.apply_transformation.assert_called_once_with(output_df, config)
	output_df.dropna.assert_called_once_with(subset=config.primary_keys)
	output_df.dropDuplicates.assert_called_once_with(config.primary_keys)
	writer.assert_called_once_with(df=output_df, spark=spark, config=config)


@pytest.mark.parametrize(
	("write_strategy", "uses_watermark"),
	[
		(WriteStrategy.APPEND, True),
		(WriteStrategy.MERGE, False),
	],
)
def test_process_base_table_watermarks_only_streaming_append(
	monkeypatch, write_strategy, uses_watermark
):
	config = SimpleNamespace(
		source_topic="topic-a",
		table_name="TABLE_A",
		schema=StructType([]),
		computed_columns=None,
		primary_keys=["id"],
		ingestion_type=DataIngestionType.STREAMING_INGESTION,
		write_strategy=write_strategy,
		time_column="kafka_timestamp",
	)
	flattened_df = Mock(name="flattened_df")
	transformed_df = Mock(name="transformed_df")
	transformed_df.dropna.return_value = transformed_df
	watermarked_df = Mock(name="watermarked_df")
	deduplicated_df = Mock(name="deduplicated_df")
	transformed_df.withWatermark.return_value = watermarked_df
	watermarked_df.dropDuplicatesWithinWatermark.return_value = deduplicated_df
	job = silverJob(Mock())
	job.read_bronze_source = Mock(return_value=flattened_df)
	job.apply_transformation = Mock(return_value=transformed_df)
	writer = Mock(return_value="writer-result")
	monkeypatch.setattr(silver_job_module.DeltaWriterEngine, "silver_writer", writer)

	result = job.process_base_table(config)

	assert result == "writer-result"
	job.read_bronze_source.assert_called_once_with(
		source_topic="topic-a",
		ingestion_type=DataIngestionType.STREAMING_INGESTION,
		schema=config.schema,
	)
	job.apply_transformation.assert_called_once_with(flattened_df, config)
	transformed_df.dropna.assert_called_once_with(subset=config.primary_keys)
	if uses_watermark:
		transformed_df.withWatermark.assert_called_once_with("kafka_timestamp", "30 minutes")
		watermarked_df.dropDuplicatesWithinWatermark.assert_called_once_with(config.primary_keys)
		writer.assert_called_once_with(df=deduplicated_df, spark=job.sparkSession, config=config)
	else:
		transformed_df.withWatermark.assert_not_called()
		watermarked_df.dropDuplicatesWithinWatermark.assert_not_called()
		writer.assert_called_once_with(df=transformed_df, spark=job.sparkSession, config=config)


@pytest.mark.parametrize(
	("write_strategy", "uses_watermark"),
	[
		(WriteStrategy.APPEND, True),
		(WriteStrategy.MERGE, False),
	],
)
def test_process_derived_streaming_watermarks_only_append(
	monkeypatch, write_strategy, uses_watermark
):
	output_df = Mock(name="output_df")
	output_df.dropna.return_value = output_df
	watermarked_df = Mock(name="watermarked_df")
	deduplicated_df = Mock(name="deduplicated_df")
	output_df.withWatermark.return_value = watermarked_df
	watermarked_df.dropDuplicatesWithinWatermark.return_value = deduplicated_df
	transform = Mock(return_value=output_df)
	config = DerivedTableConfig(
		table_name="DERIVED_TABLE",
		source_table={},
		ingestion_type=DataIngestionType.STREAMING_INGESTION,
		write_strategy=write_strategy,
		transformation_fn=transform,
		primary_keys=["id"],
	)
	job = silverJob(SimpleNamespace())
	job.apply_transformation = Mock(return_value=output_df)
	writer = Mock(return_value="writer-result")
	monkeypatch.setattr(silver_job_module.DeltaWriterEngine, "silver_writer", writer)

	result = job.process_derived_table(config)

	assert result == "writer-result"
	transform.assert_called_once_with({})
	job.apply_transformation.assert_called_once_with(output_df, config)
	output_df.dropna.assert_called_once_with(subset=config.primary_keys)
	if uses_watermark:
		output_df.withWatermark.assert_called_once_with("kafka_timestamp", "30 minutes")
		watermarked_df.dropDuplicatesWithinWatermark.assert_called_once_with(config.primary_keys)
		writer.assert_called_once_with(df=deduplicated_df, spark=job.sparkSession, config=config)
	else:
		output_df.withWatermark.assert_not_called()
		watermarked_df.dropDuplicatesWithinWatermark.assert_not_called()
		writer.assert_called_once_with(df=output_df, spark=job.sparkSession, config=config)
