from transform.common.writer.base import WriterConfig, base
from pyspark.sql.streaming import StreamingQuery
from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()


class DeltaBatchWriter(base):
    def _write(
        self,
        writerConfig: WriterConfig,
    ):
        logger.info(f"Starting Delta Batch Writer to path '{writerConfig.deltaPath}'.")

        writer = writerConfig.df.write.format("delta").mode(writerConfig.outputMode)
        if writerConfig.partitionCols:
            writer = writer.partitionBy(*writerConfig.partitionCols)

        writer.save(writerConfig.deltaPath)
