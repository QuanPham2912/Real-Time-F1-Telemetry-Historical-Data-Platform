import posixpath
from pyspark.sql import DataFrame
from metadata.logger import ETLLogger
from transform.common.writer import DeltaBatchWriter
from transform.gold.gold_table_config import GOLD_TABLE_CONFIGS, GoldTableConfig

logger = ETLLogger.get_logger()

class goldJob:
    def __init__(self, sparkSession,
                 goldBasePath :str = "s3a://f1-gold/",
                 silverBasePath :str = "s3a://f1-silver/"):
        self.sparkSession = sparkSession
        self.goldBasePath = goldBasePath
        self.silverBasePath = silverBasePath

    def read_silver_source(self, silver_source : str) -> DataFrame:
        silver_path = posixpath.join(self.silverBasePath, silver_source)
        silver_df = self.sparkSession.read.format("delta").load(silver_path)
        return silver_df

    def read_gold_source(self, gold_source : str) -> DataFrame:
            gold_path = posixpath.join(self.goldBasePath, gold_source)
            gold_df = self.sparkSession.read.format("delta").load(gold_path)
            return gold_df

    def process_gold_table(self, config :GoldTableConfig):

        gold_path = posixpath.join(self.goldBasePath, config.table_name)

        logger.info(f"Processing gold table: {config.table_name}")

        sources = {}
        if config.silver_sources:
            for silver_source in config.silver_sources:
                source_df = self.read_silver_source(silver_source = silver_source)
                sources[silver_source] = source_df
        if config.gold_sources:
            for gold_source in config.gold_sources:
                source_df = self.read_gold_source(gold_source = gold_source)
                sources[gold_source] = source_df

        gold_df = config.transformation_fn(sources)
        gold_df = gold_df.dropDuplicates(config.primary_keys)

        DeltaBatchWriter.write_to_delta(df = gold_df,
                                        deltaPath = gold_path,
                                        outputMode = config.write_mode,
                                        partitionCols = config.partition_cols)
        


    