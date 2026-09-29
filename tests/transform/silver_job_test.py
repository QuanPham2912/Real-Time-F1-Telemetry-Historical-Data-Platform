from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

import transform.silver_job as silver_job_module
from metadata.IngestionMode import DataIngestionType
from transform.silver_table_config import DerivedTableConfig
from transform.silver_job import silverJob


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
	getattr(reader, "format").return_value.load.return_value = bronze_df
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
	output_df.dropDuplicates.return_value = output_df
	output_df.withColumn.return_value = output_df
	transform = Mock(return_value=output_df)
	config = DerivedTableConfig(
		table_name="XWALK_DRIVER",
		source_table={},
		silver_source_table={"XWALK_CONSTRUCTOR": schema},
		ingestion_type=DataIngestionType.BATCH_INGESTION,
		transformation_fn=transform,
		primary_keys=["source", "source_native_key"],
	)
	read = Mock()
	read.format.return_value.load.return_value = source_df
	spark = SimpleNamespace(read=read)
	writer = Mock(return_value="write-result")
	monkeypatch.setattr(silver_job_module.DeltaBatchWriter, "write_to_delta", writer)
	monkeypatch.setattr(silver_job_module, "current_timestamp", Mock(return_value="now"))
	job = silverJob(spark, silverBasePath="s3a://silver/")
	job.apply_transformation = Mock(return_value=output_df)

	result = job.process_derived_table(config)

	assert result == "write-result"
	read.format.assert_called_once_with("delta")
	read.format.return_value.load.assert_called_once_with("s3a://silver/XWALK_CONSTRUCTOR")
	transform.assert_called_once_with({"XWALK_CONSTRUCTOR": source_df})
	writer.assert_called_once_with(
		df=output_df,
		deltaPath="s3a://silver/XWALK_DRIVER",
		partitionCols=None,
	)
