from .batch_dynamic_writer import DeltaBatchDynamicWriter
from .batch_writer import DeltaBatchWriter
from .streaming_writer import DeltaStreamWriter
from .batch_merge_writer import DeltaBatchMergeWriter
from .streaming_upsert_writer import DeltaStreamUpsertWriter
from metadata.IngestionMode import DataIngestionType
from metadata.write_strategy import WriteStrategy

WRITER_REGISTRY = {
    (DataIngestionType.STREAMING_INGESTION, WriteStrategy.APPEND): DeltaStreamWriter,
    (
        DataIngestionType.STREAMING_INGESTION,
        WriteStrategy.MERGE,
    ): DeltaStreamUpsertWriter,
    (DataIngestionType.BATCH_INGESTION, WriteStrategy.MERGE): DeltaBatchMergeWriter,
    (
        DataIngestionType.BATCH_INGESTION,
        WriteStrategy.DYNAMIC_OVERWRITE,
    ): DeltaBatchDynamicWriter,
    (DataIngestionType.BATCH_INGESTION, WriteStrategy.APPEND): DeltaBatchWriter,
}


def get_writer(mode, strategy):
    writer_class = WRITER_REGISTRY.get((mode, strategy))
    if not writer_class:
        raise ValueError(f"Writer type '{mode}_{strategy}' is not supported.")
    return writer_class()
