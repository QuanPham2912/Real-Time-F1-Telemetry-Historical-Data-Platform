from unittest.mock import Mock, call

import transform.writer as writer_module


def build_stream_writer():
	stream_writer = Mock()
	stream_writer.format.return_value = stream_writer
	stream_writer.outputMode.return_value = stream_writer
	stream_writer.option.return_value = stream_writer
	stream_writer.partitionBy.return_value = stream_writer
	stream_writer.start.return_value = "streaming-query"
	return stream_writer


def test_write_to_delta_uses_default_output_mode_without_partitions(monkeypatch):
	stream_writer = build_stream_writer()
	dataframe = Mock()
	dataframe.writeStream = stream_writer
	logger = Mock()
	monkeypatch.setattr(writer_module, "logger", logger)

	result = writer_module.DeltaStreamWriter.write_to_delta(
		dataframe,
		"/data/delta",
		"/data/checkpoints",
	)

	assert result == "streaming-query"
	logger.info.assert_called_once_with(
		"Starting Delta Stream Writer to path '/data/delta' "
		"with checkpoint at '/data/checkpoints'."
	)
	assert stream_writer.method_calls == [
		call.format("delta"),
		call.outputMode("append"),
		call.option("checkpointLocation", "/data/checkpoints"),
		call.start("/data/delta"),
	]
	stream_writer.partitionBy.assert_not_called()


def test_write_to_delta_applies_custom_mode_and_partitions(monkeypatch):
	stream_writer = build_stream_writer()
	dataframe = Mock()
	dataframe.writeStream = stream_writer
	monkeypatch.setattr(writer_module, "logger", Mock())

	result = writer_module.DeltaStreamWriter.write_to_delta(
		dataframe,
		"/data/delta",
		"/data/checkpoints",
		outputMode="complete",
		partitionCols=["season", "driver"],
	)

	assert result == "streaming-query"
	assert stream_writer.method_calls == [
		call.format("delta"),
		call.outputMode("complete"),
		call.option("checkpointLocation", "/data/checkpoints"),
		call.partitionBy("season", "driver"),
		call.start("/data/delta"),
	]
