import posixpath
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, expr, current_timestamp, lit
from metadata.logger import ETLLogger
from transform.silver_table_config import BaseIngestionConfig, BASE_TABLE_CONFIGS, DerivedTableConfig, DERIVED_TABLE_CONFIGS
from transform.writer import DeltaStreamWriter, DeltaBatchWriter
from metadata.IngestionMode import DataIngestionType
from typing import Dict, Any, Union
from pyspark.sql.types import StructType

logger = ETLLogger.get_logger()

class silverJob:
    def __init__(self, sparkSession,
                 bronzeBasePath :str = "s3a://f1-bronze/",
                 silverBasePath :str = "s3a://f1-silver/"):
        self.sparkSession = sparkSession
        self.bronzeBasePath = bronzeBasePath
        self.silverBasePath = silverBasePath

    def read_bronze_source(self, source_topic :str, ingestion_type :str, schema: StructType) -> DataFrame:
        bronze_path = posixpath.join(self.bronzeBasePath, source_topic)
        if ingestion_type == DataIngestionType.STREAMING_INGESTION:
            bronze_df = self.sparkSession.readStream.format("delta").load(bronze_path)
        elif ingestion_type == DataIngestionType.BATCH_INGESTION:
            bronze_df = self.sparkSession.read.format("delta").load(bronze_path)

        parsed_df = bronze_df.withColumn("data", from_json(col("kafka_value"), schema))

        flattened_df = parsed_df.select(
                    col("data.*"),
                    col("kafka_timestamp"),
                    col("ingest_time").alias("bronze_ingest_time"),
                    current_timestamp().alias("silver_load_time")
                )

        return flattened_df

    def apply_transformation(self, flattened_df : DataFrame, config: Union[BaseIngestionConfig, DerivedTableConfig]) -> DataFrame:
         #Apply Column Mapping
        if config.column_mapping:
            for old_col, new_col in config.column_mapping.items():
                if old_col in flattened_df.columns:
                    flattened_df = flattened_df.withColumnRenamed(old_col, new_col)
        
        #Convert all column names to lowercase
        flattened_df = flattened_df.toDF(*[c.lower() for c in flattened_df.columns])
        return flattened_df

    def process_base_table(self, config: BaseIngestionConfig):

        bronze_path = posixpath.join(self.bronzeBasePath, config.source_topic)
        silver_path = posixpath.join(self.silverBasePath, config.table_name)
        checkpoint_path = posixpath.join(self.silverBasePath, "checkpoints", config.table_name)

        logger.info(f"Processing Silver Base Table: {config.table_name} from {bronze_path}")

        flattened_df = self.read_bronze_source(source_topic=config.source_topic,
                                               ingestion_type=config.ingestion_type,
                                               schema=config.schema)
        flattened_df = self.apply_transformation(flattened_df, config)

        #Using Computed Columns
        if config.computed_columns:
            for col_name, sql_expr in config.computed_columns.items():
                if col_name in flattened_df.columns:
                    flattened_df = flattened_df.withColumn(col_name, expr(sql_expr))

        #Deduplicate
        flattened_df = flattened_df.dropDuplicates(config.primary_keys)

        logger.info(f"Writing to silver layer at path '{silver_path}' with checkpoint at '{checkpoint_path}'.")

        if config.ingestion_type == DataIngestionType.STREAMING_INGESTION:
            return DeltaStreamWriter.write_to_delta(df = flattened_df,
                                                    deltaPath = silver_path,
                                                    checkPointPath = checkpoint_path,
                                                    partitionCols = config.partition_cols)
        elif config.ingestion_type == DataIngestionType.BATCH_INGESTION:
            return DeltaBatchWriter.write_to_delta(df = flattened_df,
                                                   deltaPath = silver_path,
                                                   partitionCols = config.partition_cols)

    def process_derived_table(self, config: DerivedTableConfig):
        silver_path = posixpath.join(self.silverBasePath, config.table_name)
        checkpoint_path = posixpath.join(self.silverBasePath, "checkpoints", config.table_name)

        logger.info(f"Processing Silver derived Table: {config.table_name}")


        sources = {}
        if config.source_table:
            for config_source_topic, config_schema  in config.source_table.items():
                flattened_df = self.read_bronze_source(source_topic = config_source_topic,
                                                       ingestion_type = config.ingestion_type,
                                                       schema = config_schema)

                sources[config_source_topic] = flattened_df

        if config.silver_source_table:
            for source_topic, schema  in config.silver_source_table.items():
                source_silver_path = posixpath.join(self.silverBasePath, source_topic)
                if config.ingestion_type == DataIngestionType.STREAMING_INGESTION:
                    flattened_df = self.sparkSession.readStream.format("delta").load(source_silver_path)
                elif config.ingestion_type == DataIngestionType.BATCH_INGESTION:
                    flattened_df = self.sparkSession.read.format("delta").load(source_silver_path)
        
                sources[source_topic] = flattened_df
        

        transformed_df = config.transformation_fn(sources)

        transformed_df = self.apply_transformation(transformed_df, config)

        transformed_df = transformed_df.dropDuplicates(config.primary_keys)

        transformed_df = transformed_df.withColumn("silver_load_time", current_timestamp())

        logger.info(f"Writing to silver layer at path '{silver_path}' with checkpoint at '{checkpoint_path}'.")
        if config.ingestion_type == DataIngestionType.STREAMING_INGESTION:
            return DeltaStreamWriter.write_to_delta(df = transformed_df,
                                                    deltaPath = silver_path,
                                                    checkPointPath = checkpoint_path,
                                                    partitionCols = config.partition_cols)
        elif config.ingestion_type == DataIngestionType.BATCH_INGESTION:
            return DeltaBatchWriter.write_to_delta(df = transformed_df,
                                                   deltaPath = silver_path,
                                                   partitionCols = config.partition_cols)

        
