from unittest.mock import Mock, call

import transform.bronze_job as bronze_module
from metadata.f1_topic import F1Topic


def build_kafka_reader():
	reader = Mock()
	reader.format.return_value = reader
	reader.option.return_value = reader
	reader.load.return_value = "kafka-dataframe"
	return reader


def test_read_from_kafka_configures_stream_source():
	reader = build_kafka_reader()
	spark_session = Mock()
	spark_session.readStream = reader
	job = bronze_module.BronzeJob(spark_session, bootstrapServer="kafka:29092")

	result = job.read_from_kafka(F1Topic.STREAM_TELEMETRY)

	assert result == "kafka-dataframe"
	assert reader.method_calls == [
		call.format("kafka"),
		call.option("kafka.bootstrap.servers", "kafka:29092"),
		call.option("subscribe", F1Topic.STREAM_TELEMETRY.value),
		call.option("startingOffsets", "earliest"),
		call.load(),
	]


def test_add_bronze_metadata_selects_expected_columns(monkeypatch):
	dataframe = Mock()
	dataframe.select.return_value = "bronze-dataframe"
	kafka_key = Mock(name="kafka_key")
	kafka_value = Mock(name="kafka_value")
	kafka_timestamp = Mock(name="kafka_timestamp")
	ingest_time = Mock(name="ingest_time")
	ingest_date = Mock(name="ingest_date")
	topic = Mock(name="topic")
	current_time = Mock(name="current_time")

	monkeypatch.setattr(bronze_module, "col", Mock(side_effect=[kafka_key, kafka_value, kafka_timestamp]))
	monkeypatch.setattr(bronze_module, "current_timestamp", Mock(return_value=current_time))
	monkeypatch.setattr(bronze_module, "to_date", Mock(return_value=ingest_date))
	monkeypatch.setattr(bronze_module, "lit", Mock(return_value=topic))
	kafka_key.cast.return_value.alias.return_value = "kafka_key_column"
	kafka_value.cast.return_value.alias.return_value = "kafka_value_column"
	kafka_timestamp.alias.return_value = "kafka_timestamp_column"
	current_time.alias.return_value = ingest_time
	ingest_date.alias.return_value = "ingest_date_column"
	topic.alias.return_value = "topic_column"
	job = bronze_module.BronzeJob(Mock())

	result = job.add_bronze_metadata(dataframe, F1Topic.FACT_LAP)

	assert result == "bronze-dataframe"
	assert dataframe.select.call_args.args == (
		"kafka_key_column",
		"kafka_value_column",
		"kafka_timestamp_column",
		ingest_time,
		"ingest_date_column",
		"topic_column",
	)
	assert bronze_module.col.call_args_list == [
		call("key"),
		call("value"),
		call("timestamp"),
	]
	bronze_module.lit.assert_called_once_with(F1Topic.FACT_LAP.value)


def test_write_to_bronze_builds_paths_and_delegates_to_delta_writer(monkeypatch):
	raw_dataframe = Mock(name="raw_dataframe")
	bronze_dataframe = Mock(name="bronze_dataframe")
	spark_session = Mock()
	job = bronze_module.BronzeJob(spark_session, basePath="s3a://f1-bronze/")
	read_from_kafka = Mock(return_value=raw_dataframe)
	add_bronze_metadata = Mock(return_value=bronze_dataframe)
	delta_writer = Mock(return_value="streaming-query")
	logger = Mock()
	monkeypatch.setattr(job, "read_from_kafka", read_from_kafka)
	monkeypatch.setattr(job, "add_bronze_metadata", add_bronze_metadata)
	monkeypatch.setattr(bronze_module.DeltaStreamWriter, "write_to_delta", delta_writer)
	monkeypatch.setattr(bronze_module, "logger", logger)

	result = job.write_to_bronze(F1Topic.STREAM_WEATHER)

	assert result == "streaming-query"
	read_from_kafka.assert_called_once_with(F1Topic.STREAM_WEATHER)
	add_bronze_metadata.assert_called_once_with(raw_dataframe, F1Topic.STREAM_WEATHER)
	delta_writer.assert_called_once_with(
		bronze_dataframe,
		"s3a://f1-bronze/f1.stream.weather",
		"s3a://f1-bronze/checkpoints/f1.stream.weather",
		partitionCols=["ingest_date"],
	)
	logger.info.assert_called_once_with(
		"Writing to bronze layer at path 's3a://f1-bronze/f1.stream.weather' "
		"with checkpoint at 's3a://f1-bronze/checkpoints/f1.stream.weather'."
	)
