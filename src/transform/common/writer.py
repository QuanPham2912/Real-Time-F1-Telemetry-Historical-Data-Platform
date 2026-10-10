from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable
from typing import List, Dict, Optional, Callable, TYPE_CHECKING
from metadata.logger import ETLLogger
import posixpath
from transform.silver.silver_table_config import Base
from pyspark.sql.functions import row_number, col

logger = ETLLogger.get_logger()
silverBasePath :str = "s3a://f1-silver/"

class DeltaStreamWriter:
    @staticmethod
    def write_to_delta(df: DataFrame,
                       deltaPath: str,
                       checkPointPath: str,
                       outputMode: str = "append", # mode "append" Because writeStream mainly use for FACT table and this writeStream also be used for bronze layer 
                       partitionCols: list[str] | None = None) -> StreamingQuery:
        logger.info(f"Starting Delta Stream Writer to path '{deltaPath}' with checkpoint at '{checkPointPath}'.")

        writer = df.writeStream \
            .format("delta") \
            .outputMode(outputMode) \
            .option("checkpointLocation", checkPointPath) \
            .trigger(availableNow = True)

        if partitionCols:
            writer = writer.partitionBy(*partitionCols)

        return writer.start(deltaPath)

    @staticmethod
    def merge_to_delta(sparkSession : SparkSession,
                           df : DataFrame,
                           destinationPath : str,
                           checkPointPath :str,
                           primaryKeys : list[str],
                           partitionColumns: list[str] | None = None):
            logger.info(f"Starting Delta Straming merge with forEach to path '{destinationPath}'.")
            def upsert_batch(batch_df: DataFrame, batch_id: int):
                if batch_df.isEmpty():
                    return

                #Take the latest record for each primary key based on kafka_timestamp
                # This is to ensure that if there are multiple records for the same primary key in the batch, only the latest one is considered for merging into the Delta table.
                # This also helps eliminate the need to use dropDuplicateWithWaterMark.
                w = Window.partitionBy(*primaryKeys).orderBy(col("kafka_timestamp").desc())
                batch_df = (batch_df
                    .withColumn("_rn", row_number().over(w))
                    .filter("_rn = 1")
                    .drop("_rn"))

                if DeltaTable.isDeltaTable(sparkSession, destinationPath):
                    delta_table = DeltaTable.forPath(sparkSession, destinationPath)
                    conditions = [f"target.{pk} = source.{pk}" for pk in primaryKeys]
                    if partitionColumns:
                        conditions.extend([f"target.{col} = source.{col}" for col in partitionColumns])

                    join_condition = " AND ".join(conditions)
                    delta_table.alias("target") \
                        .merge(batch_df.alias("source"), join_condition) \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute() 
                else:
                    DeltaBatchWriter.write_to_delta(df = batch_df, deltaPath = destinationPath, partitionCols = partitionColumns )
            writer = df.writeStream \
                        .foreachBatch(upsert_batch) \
                        .option("checkpointLocation", checkPointPath) \
                        .trigger(availableNow = True) \
                        .start()
            return writer

class DeltaBatchWriter:
    @staticmethod
    def write_to_delta(df: DataFrame,
                        deltaPath: str,
                        outputMode: str = "append",
                        partitionCols: list[str] | None = None):
        logger.info(f"Starting Delta Batch Writer to path '{deltaPath}'.")
    
        writer = df.write \
            .format("delta") \
            .mode(outputMode)    
        if partitionCols:
            writer = writer.partitionBy(*partitionCols)
    
        writer.save(deltaPath)

    @staticmethod
    def dynamic_partition_overwrite(df: DataFrame,
                                    deltaPath: str,
                                    replaceConditionCols: list[str] | None = None,
                                    partitionCols: list[str] | None = None,
                                ):
        logger.info(f"Starting Delta Dynamic Partition Overwrite to path '{deltaPath}'.")
        def build_replace_condition(df, partition_cols: list) -> str:
            conditions = []
            for col_name in partition_cols:
                #Ingest distinct value from partition cols
                distint_vals = [row[col_name] for row in df.select(col_name).distinct().collect()]
                formatted_vals = [f"'{val}'" if isinstance(val, str) else str(val) for val in distint_vals] 

                if len(formatted_vals) > 0:
                    conditions.append(f"{col_name} IN ({', '.join(formatted_vals)})")
            return " AND ".join(conditions)
        replaceCondition = build_replace_condition(df, replaceConditionCols) if replaceConditionCols else None
        writer = df.write \
            .format("delta") \
            .mode("overwrite")    
        if partitionCols:
            writer = writer.partitionBy(*partitionCols)
        if replaceCondition:
            writer = writer.option("replaceWhere", replaceCondition)
    
        writer.save(deltaPath)   

    @staticmethod
    def merge_to_delta(sparkSession : SparkSession,
                       dataframe : DataFrame,
                       destinationPath : str,
                       primaryKeys : list[str],
                       partitionColumns: list[str] | None = None):
        if DeltaTable.isDeltaTable(sparkSession, destinationPath):
            logger.info(f"Starting Delta Batch Merge to path '{destinationPath}'.")
            delta_table = DeltaTable.forPath(sparkSession, destinationPath)
            conditions = [f"target.{pk} = source.{pk}" for pk in primaryKeys]
            if partitionColumns:
                conditions.extend([f"target.{col} = source.{col}" for col in partitionColumns])

            join_condition = " AND ".join(conditions)

            # if data already exits in delta lake this command will overwrite match data by the newest
            delta_table.alias("target") \
                .merge(dataframe.alias("source"), join_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute() 
        else:
            DeltaBatchWriter.write_to_delta(df = dataframe, deltaPath = destinationPath, partitionCols = partitionColumns )

class DeltaWriterEngine:
    @staticmethod
    def silver_writer(
        df : DataFrame,
        spark : SparkSession,
        config : Base,
    ):
        mode = config.ingestion_type.lower()
        strategy = config.write_strategy.lower()
        silver_path = posixpath.join(silverBasePath, config.table_name)
        if mode == "streaming_ingestion":
            checkpoint_path = posixpath.join(silverBasePath, "checkpoints", config.table_name)
            if strategy == "append":
                return DeltaStreamWriter.write_to_delta(df = df,
                                                        deltaPath = silver_path,
                                                        checkPointPath = checkpoint_path,
                                                        partitionCols = config.partition_cols)
            elif strategy == "merge":
                return DeltaStreamWriter.merge_to_delta(sparkSession = spark,
                                                        df = df,
                                                        destinationPath = silver_path,
                                                        checkPointPath = checkpoint_path,
                                                        partitionColumns = config.partition_cols,
                                                        primaryKeys = config.primary_keys)
        elif mode == "batch_ingestion":
            if strategy == "merge":
                DeltaBatchWriter.merge_to_delta(sparkSession = spark,
                                                 dataframe = df,
                                                 destinationPath = silver_path,
                                                 partitionColumns = config.partition_cols,
                                                 primaryKeys = config.primary_keys)
            elif strategy == "dynamic_overwrite":
                DeltaBatchWriter.dynamic_partition_overwrite(df = df,
                                                             deltaPath = silver_path,
                                                             replaceConditionCols = config.replace_condition_cols,
                                                             partitionCols = config.partition_cols)
            elif strategy == "append":
                DeltaBatchWriter.write_to_delta(df = df,
                                                deltaPath = silver_path,
                                                partitionCols = config.partition_cols)

            
        
