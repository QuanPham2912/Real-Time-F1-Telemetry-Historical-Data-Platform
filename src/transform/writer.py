from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()

class DeltaStreamWriter:
    @staticmethod
    def write_to_delta(df: DataFrame,
                       deltaPath: str,
                       checkPointPath: str,
                       outputMode: str = "append",
                       partitionCols: list[str] | None = None) -> StreamingQuery:
        logger.info(f"Starting Delta Stream Writer to path '{deltaPath}' with checkpoint at '{checkPointPath}'.")

        writer = df.writeStream \
            .format("delta") \
            .outputMode(outputMode) \
            .option("checkpointLocation", checkPointPath)

        if partitionCols:
            writer = writer.partitionBy(*partitionCols)

        return writer.start(deltaPath)
