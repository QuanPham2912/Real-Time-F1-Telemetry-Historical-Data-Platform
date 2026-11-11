from transform.common.writer.base import WriterConfig, base
from pyspark.sql.streaming import StreamingQuery
from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()


class DeltaStreamWriter(base):
    def _write(
        self,
        writerConfig: WriterConfig,
    ) -> StreamingQuery:
        self.validate_required(writerConfig, "checkPointPath")
        logger.info(
            f"Starting Delta Stream Writer to path '{writerConfig.deltaPath}' with checkpoint at '{writerConfig.checkPointPath}'."
        )

        writer = (
            writerConfig.df.writeStream.format("delta")
            .outputMode(writerConfig.outputMode)
            .option("checkpointLocation", writerConfig.checkPointPath)
            .trigger(availableNow=True)
        )

        if writerConfig.partitionCols:
            writer = writer.partitionBy(*writerConfig.partitionCols)

        return writer.start(writerConfig.deltaPath)
