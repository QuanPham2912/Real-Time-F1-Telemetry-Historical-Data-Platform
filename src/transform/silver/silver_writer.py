from pyspark.sql import DataFrame, SparkSession
from metadata.logger import ETLLogger
import posixpath
from transform.silver.silver_table_config import Base
from metadata.setting import get_settings
from transform.common.writer import get_writer

logger = ETLLogger.get_logger()
settings = get_settings()


class DeltaWriterEngine:
    @staticmethod
    def silver_writer(
        df: DataFrame,
        spark: SparkSession,
        config: Base,
    ):
        silver_base_path = settings.silver_base_path
        silver_path = posixpath.join(silver_base_path, config.table_name)

        writer = get_writer(config.ingestion_type, config.write_strategy)
        checkpoint_path = posixpath.join(
            silver_base_path, "checkpoints", config.table_name
        )

        return writer.write(
            df=df,
            deltaPath=silver_path,
            checkPointPath=checkpoint_path,
            partitionCols=config.partition_cols,
            primaryKeys=config.primary_keys,
            replaceConditionCols=config.replace_condition_cols,
            sparkSession=spark,
        )
