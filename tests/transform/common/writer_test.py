import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, call, patch

delta_package = ModuleType("delta")
delta_package.__path__ = []
delta_tables = ModuleType("delta.tables")
delta_tables.DeltaTable = Mock()
with patch.dict(sys.modules, {"delta": delta_package, "delta.tables": delta_tables}):
    import transform.common.writer.batch_dynamic_writer as dynamic_module
    import transform.common.writer.batch_merge_writer as batch_merge_module
    import transform.common.writer.batch_writer as batch_module
    import transform.common.writer.streaming_upsert_writer as upsert_module
    import transform.common.writer.streaming_writer as stream_module
    import transform.silver.silver_writer as silver_writer_module


def build_stream_writer():
    stream_writer = Mock()
    stream_writer.format.return_value = stream_writer
    stream_writer.outputMode.return_value = stream_writer
    stream_writer.option.return_value = stream_writer
    stream_writer.partitionBy.return_value = stream_writer
    stream_writer.trigger.return_value = stream_writer
    stream_writer.start.return_value = "streaming-query"
    return stream_writer


def build_batch_writer():
    batch_writer = Mock()
    batch_writer.format.return_value = batch_writer
    batch_writer.mode.return_value = batch_writer
    batch_writer.partitionBy.return_value = batch_writer
    batch_writer.option.return_value = batch_writer
    return batch_writer


def build_delta_table():
    delta_table = Mock()
    (
        delta_table.alias.return_value.merge.return_value
        .whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value
        .execute.return_value
    ) = None
    return delta_table


def stub_latest_record_window(monkeypatch, batch_df):
    window = Mock()
    window_spec = Mock()
    window.partitionBy.return_value.orderBy.return_value = window_spec
    window_api = Mock()
    window_api.partitionBy.return_value = window
    timestamp_column = Mock()
    timestamp_column.desc.return_value = "descending-timestamp"
    rank_expression = Mock()
    rank_expression.over.return_value = "rank-expression"
    ranked_df = Mock(name="ranked_df")
    filtered_df = Mock(name="filtered_df")
    latest_df = Mock(name="latest_df")
    batch_df.withColumn.return_value = ranked_df
    ranked_df.filter.return_value = filtered_df
    filtered_df.drop.return_value = latest_df
    monkeypatch.setattr(upsert_module, "Window", window_api)
    monkeypatch.setattr(upsert_module, "col", Mock(return_value=timestamp_column))
    monkeypatch.setattr(upsert_module, "row_number", Mock(return_value=rank_expression))
    return {
        "window": window,
        "window_api": window_api,
        "timestamp_column": timestamp_column,
        "rank_expression": rank_expression,
        "ranked_df": ranked_df,
        "filtered_df": filtered_df,
        "latest_df": latest_df,
    }


def test_stream_writer_uses_default_mode_without_partitions(monkeypatch):
    stream_writer = build_stream_writer()
    dataframe = Mock()
    dataframe.writeStream = stream_writer
    logger = Mock()
    monkeypatch.setattr(stream_module, "logger", logger)

    result = stream_module.DeltaStreamWriter().write(
        df=dataframe,
        deltaPath="/data/delta",
        checkPointPath="/data/checkpoints",
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
        call.trigger(availableNow=True),
        call.start("/data/delta"),
    ]
    stream_writer.partitionBy.assert_not_called()


def test_stream_writer_applies_custom_mode_and_partitions(monkeypatch):
    stream_writer = build_stream_writer()
    dataframe = Mock()
    dataframe.writeStream = stream_writer
    monkeypatch.setattr(stream_module, "logger", Mock())

    result = stream_module.DeltaStreamWriter().write(
        df=dataframe,
        deltaPath="/data/delta",
        checkPointPath="/data/checkpoints",
        outputMode="complete",
        partitionCols=["season", "driver"],
    )

    assert result == "streaming-query"
    assert stream_writer.method_calls == [
        call.format("delta"),
        call.outputMode("complete"),
        call.option("checkpointLocation", "/data/checkpoints"),
        call.trigger(availableNow=True),
        call.partitionBy("season", "driver"),
        call.start("/data/delta"),
    ]


def test_batch_writer_uses_default_mode_without_partitions(monkeypatch):
    batch_writer = build_batch_writer()
    dataframe = Mock()
    dataframe.write = batch_writer
    logger = Mock()
    monkeypatch.setattr(batch_module, "logger", logger)

    result = batch_module.DeltaBatchWriter().write(
        df=dataframe,
        deltaPath="/data/delta",
    )

    assert result is None
    logger.info.assert_called_once_with(
        "Starting Delta Batch Writer to path '/data/delta'."
    )
    assert batch_writer.method_calls == [
        call.format("delta"),
        call.mode("append"),
        call.save("/data/delta"),
    ]
    batch_writer.partitionBy.assert_not_called()


def test_batch_writer_applies_custom_mode_and_partitions(monkeypatch):
    batch_writer = build_batch_writer()
    dataframe = Mock()
    dataframe.write = batch_writer
    monkeypatch.setattr(batch_module, "logger", Mock())

    batch_module.DeltaBatchWriter().write(
        df=dataframe,
        deltaPath="/data/delta",
        outputMode="overwrite",
        partitionCols=["season", "driver"],
    )

    assert batch_writer.method_calls == [
        call.format("delta"),
        call.mode("overwrite"),
        call.partitionBy("season", "driver"),
        call.save("/data/delta"),
    ]


def test_stream_upsert_merges_nonempty_batches_and_skips_empty_batches(monkeypatch):
    stream_writer = Mock()
    stream_writer.foreachBatch.return_value = stream_writer
    stream_writer.option.return_value = stream_writer
    stream_writer.trigger.return_value = stream_writer
    stream_writer.start.return_value = "streaming-query"
    callbacks = {}

    def register_callback(callback):
        callbacks["batch"] = callback
        return stream_writer

    stream_writer.foreachBatch.side_effect = register_callback
    dataframe = Mock()
    dataframe.writeStream = stream_writer
    delta_table = build_delta_table()
    spark = Mock()
    monkeypatch.setattr(upsert_module, "logger", Mock())
    monkeypatch.setattr(upsert_module.DeltaTable, "isDeltaTable", Mock(return_value=True))
    monkeypatch.setattr(
        upsert_module.DeltaTable, "forPath", Mock(return_value=delta_table)
    )

    result = upsert_module.DeltaStreamUpsertWriter().write(
        sparkSession=spark,
        df=dataframe,
        deltaPath="/data/delta",
        checkPointPath="/data/checkpoints",
        primaryKeys=["event_id"],
        partitionCols=["season"],
    )
    batch_df = Mock()
    batch_df.isEmpty.return_value = False
    latest_record = stub_latest_record_window(monkeypatch, batch_df)
    callbacks["batch"](batch_df, 1)
    empty_batch = Mock()
    empty_batch.isEmpty.return_value = True
    callbacks["batch"](empty_batch, 2)

    assert result == "streaming-query"
    latest_record["window_api"].partitionBy.assert_called_once_with("event_id")
    latest_record["window"].orderBy.assert_called_once_with("descending-timestamp")
    latest_record["timestamp_column"].desc.assert_called_once_with()
    latest_record["rank_expression"].over.assert_called_once_with(
        latest_record["window"].orderBy.return_value
    )
    batch_df.withColumn.assert_called_once_with("_rn", "rank-expression")
    latest_record["ranked_df"].filter.assert_called_once_with("_rn = 1")
    latest_record["filtered_df"].drop.assert_called_once_with("_rn")
    assert stream_writer.method_calls == [
        call.foreachBatch(callbacks["batch"]),
        call.option("checkpointLocation", "/data/checkpoints"),
        call.trigger(availableNow=True),
        call.start(),
    ]
    upsert_module.DeltaTable.isDeltaTable.assert_called_once_with(spark, "/data/delta")
    delta_table.alias.return_value.merge.assert_called_once_with(
        latest_record["latest_df"].alias.return_value,
        "target.event_id = source.event_id AND target.season = source.season",
    )
    upsert_module.DeltaTable.forPath.assert_called_once()
    empty_batch.isEmpty.assert_called_once_with()


def test_stream_upsert_initializes_missing_delta_table(monkeypatch):
    stream_writer = Mock()
    stream_writer.foreachBatch.return_value = stream_writer
    stream_writer.option.return_value = stream_writer
    callbacks = {}

    def register_callback(callback):
        callbacks["batch"] = callback
        return stream_writer

    stream_writer.foreachBatch.side_effect = register_callback
    dataframe = Mock()
    dataframe.writeStream = stream_writer
    batch_df = Mock()
    batch_df.isEmpty.return_value = False
    write_delta = Mock()
    monkeypatch.setattr(upsert_module, "logger", Mock())
    monkeypatch.setattr(upsert_module.DeltaTable, "isDeltaTable", Mock(return_value=False))
    monkeypatch.setattr(upsert_module.DeltaBatchWriter, "write", write_delta)

    upsert_module.DeltaStreamUpsertWriter().write(
        sparkSession=Mock(),
        df=dataframe,
        deltaPath="/data/delta",
        checkPointPath="/data/checkpoints",
        primaryKeys=["event_id"],
        partitionCols=["season"],
    )
    latest_record = stub_latest_record_window(monkeypatch, batch_df)
    callbacks["batch"](batch_df, 1)

    write_delta.assert_called_once_with(
        df=latest_record["latest_df"],
        deltaPath="/data/delta",
        partitionCols=["season"],
    )


def test_batch_merge_uses_primary_and_partition_keys(monkeypatch):
    delta_table = build_delta_table()
    monkeypatch.setattr(batch_merge_module, "logger", Mock())
    monkeypatch.setattr(
        batch_merge_module.DeltaTable, "isDeltaTable", Mock(return_value=True)
    )
    monkeypatch.setattr(
        batch_merge_module.DeltaTable, "forPath", Mock(return_value=delta_table)
    )
    dataframe = Mock()

    batch_merge_module.DeltaBatchMergeWriter().write(
        sparkSession=Mock(),
        df=dataframe,
        deltaPath="/data/delta",
        primaryKeys=["event_id"],
        partitionCols=["season"],
    )

    delta_table.alias.return_value.merge.assert_called_once_with(
        dataframe.alias.return_value,
        "target.event_id = source.event_id AND target.season = source.season",
    )
    merge_builder = delta_table.alias.return_value.merge.return_value
    merge_builder.whenMatchedUpdateAll.assert_called_once_with()
    merge_builder.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.assert_called_once_with()
    merge_builder.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once_with()


def test_batch_merge_passes_config_to_initial_writer_when_table_is_missing(monkeypatch):
    dataframe = Mock()
    write_initial = Mock()
    monkeypatch.setattr(batch_merge_module, "logger", Mock())
    monkeypatch.setattr(
        batch_merge_module.DeltaTable, "isDeltaTable", Mock(return_value=False)
    )
    monkeypatch.setattr(batch_merge_module.DeltaBatchWriter, "_write", write_initial)
    spark = Mock()

    batch_merge_module.DeltaBatchMergeWriter().write(
        sparkSession=spark,
        df=dataframe,
        deltaPath="/data/delta",
        primaryKeys=["event_id"],
        partitionCols=["season"],
    )

    config = write_initial.call_args.args[0]
    assert config.df is dataframe
    assert config.deltaPath == "/data/delta"
    assert config.partitionCols == ["season"]


def test_dynamic_partition_overwrite_sets_replace_condition(monkeypatch):
    batch_writer = build_batch_writer()
    dataframe = Mock()
    dataframe.write = batch_writer
    dataframe.select.return_value.distinct.return_value.collect.return_value = [
        {"race_id": 2024},
        {"race_id": 2025},
    ]
    monkeypatch.setattr(dynamic_module, "logger", Mock())

    dynamic_module.DeltaBatchDynamicWriter().write(
        df=dataframe,
        deltaPath="/data/delta",
        replaceConditionCols=["race_id"],
        partitionCols=["season"],
    )

    dataframe.select.assert_called_once_with("race_id")
    assert batch_writer.method_calls == [
        call.format("delta"),
        call.mode("overwrite"),
        call.partitionBy("season"),
        call.option("replaceWhere", "race_id IN (2024, 2025)"),
        call.save("/data/delta"),
    ]


def test_silver_writer_uses_configured_path_and_forwards_writer_options(monkeypatch):
    monkeypatch.setattr(
        silver_writer_module,
        "settings",
        SimpleNamespace(silver_base_path="s3a://custom-silver/root/"),
    )
    writer = Mock()
    monkeypatch.setattr(
        silver_writer_module, "get_writer", Mock(return_value=writer)
    )
    dataframe = Mock()
    spark = Mock()
    config = SimpleNamespace(
        ingestion_type="batch_ingestion",
        write_strategy="dynamic_overwrite",
        table_name="DIM_RACE",
        replace_condition_cols=["race_id"],
        partition_cols=["season"],
        primary_keys=["race_id"],
    )

    silver_writer_module.DeltaWriterEngine.silver_writer(dataframe, spark, config)

    writer.write.assert_called_once_with(
        df=dataframe,
        deltaPath="s3a://custom-silver/root/DIM_RACE",
        checkPointPath="s3a://custom-silver/root/checkpoints/DIM_RACE",
        partitionCols=["season"],
        primaryKeys=["race_id"],
        replaceConditionCols=["race_id"],
        sparkSession=spark,
    )


def test_silver_writer_uses_settings_default_path(monkeypatch):
    monkeypatch.setattr(
        silver_writer_module,
        "settings",
        SimpleNamespace(silver_base_path="s3a://f1-silver/"),
    )
    writer = Mock()
    monkeypatch.setattr(
        silver_writer_module, "get_writer", Mock(return_value=writer)
    )
    dataframe = Mock()
    spark = Mock()
    config = SimpleNamespace(
        ingestion_type="batch_ingestion",
        write_strategy="append",
        table_name="DIM_RACE",
        replace_condition_cols=None,
        partition_cols=None,
        primary_keys=["race_id"],
    )

    silver_writer_module.DeltaWriterEngine.silver_writer(dataframe, spark, config)

    writer.write.assert_called_once_with(
        df=dataframe,
        deltaPath="s3a://f1-silver/DIM_RACE",
        checkPointPath="s3a://f1-silver/checkpoints/DIM_RACE",
        partitionCols=None,
        primaryKeys=["race_id"],
        replaceConditionCols=None,
        sparkSession=spark,
    )
