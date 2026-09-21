from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from metadata.logger import ETLLogger
from metadata import f1_topic
from transform.writer import DeltaStreamWriter
import posixpath #To connect the path to delta lake

logger = ETLLogger.get_logger()

class BronzeJob:
    def __init__(self,  sparkSession,
                 bootstrapServer: str="localhost:9092",
                 basePath :str="s3a://f1-bronze/"):
        self.sparkSession = sparkSession
        self.bootstrapServer = bootstrapServer
        self.basePath = basePath

    def read_from_kafka(self, topic: f1_topic.F1Topic) -> DataFrame:
        return self.sparkSession.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrapServer) \
            .option("subscribe", topic.value) \
            .option("startingOffsets", "earliest") \
            .load()

    # transform raw data to bronze metadata with format (key, value, timestamp, ingest_time, ingest_date, topic)
    # this data will be used to write to the bronze layer in delta lake
    def add_bronze_metadata(self, df: DataFrame, topic: f1_topic.F1Topic) -> DataFrame:
        return df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingest_time"),
            to_date(current_timestamp()).alias("ingest_date"),
            lit(topic.value).alias("topic")
        )

    def write_to_bronze(self, topic: f1_topic.F1Topic):
        raw_df = self.read_from_kafka(topic)
        bronze_df = self.add_bronze_metadata(raw_df, topic)
        delta_path = posixpath.join(self.basePath, topic.value)
        checkpoint_path = posixpath.join(self.basePath, "checkpoints", topic.value)
        partition_cols = ["ingest_date"]
        logger.info(f"Writing to bronze layer at path '{delta_path}' with checkpoint at '{checkpoint_path}'.")
        return DeltaStreamWriter.write_to_delta(bronze_df, delta_path, checkpoint_path, partitionCols=partition_cols)

