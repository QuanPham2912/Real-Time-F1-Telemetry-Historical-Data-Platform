from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from metadata.logger import ETLLogger
from metadata import f1_topic
from metadata.setting import get_settings
import posixpath
from transform.common.writer import get_writer
from metadata.IngestionMode import DataIngestionType
from metadata.write_strategy import WriteStrategy

logger = ETLLogger.get_logger()
settings = get_settings()


class BronzeJob:
    def __init__(
        self,
        sparkSession,
        bootstrapServer: str | None = None,
        basePath: str | None = None,
        writer=None,
    ):
        self.sparkSession = sparkSession
        self.bootstrapServer = (
            bootstrapServer
            if bootstrapServer is not None
            else settings.kafka_bootstrap_servers
        )
        if basePath is None:
            basePath = settings.bronze_base_path
        self.basePath = basePath
        self.writer = get_writer(
            DataIngestionType.STREAMING_INGESTION, WriteStrategy.APPEND
        )

    def read_from_kafka(self, topic: f1_topic.F1Topic) -> DataFrame:
        return (
            self.sparkSession.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrapServer)
            .option("subscribe", topic.value)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 50000)
            .load()
        )

    # transform raw data to bronze metadata with format (key, value, timestamp, ingest_time, ingest_date, topic)
    # this data will be used to write to the bronze layer in delta lake
    def add_bronze_metadata(self, df: DataFrame, topic: f1_topic.F1Topic) -> DataFrame:
        return df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingest_time"),
            to_date(current_timestamp()).alias("ingest_date"),
            lit(topic.value).alias("topic"),
        )

    def write_to_bronze(self, topic: f1_topic.F1Topic):
        raw_df = self.read_from_kafka(topic)
        bronze_df = self.add_bronze_metadata(raw_df, topic)
        delta_path = posixpath.join(self.basePath, topic.value)
        checkpoint_path = posixpath.join(self.basePath, "checkpoints", topic.value)
        partition_cols = ["ingest_date"]
        logger.info(
            f"Writing to bronze layer at path '{delta_path}' with checkpoint at '{checkpoint_path}'."
        )
        return self.writer.write(
            df=bronze_df,
            deltaPath=delta_path,
            checkPointPath=checkpoint_path,
            partitionCols=partition_cols,
        )
