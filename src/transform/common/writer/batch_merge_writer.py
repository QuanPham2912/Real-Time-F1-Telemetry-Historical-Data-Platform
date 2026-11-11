from transform.common.writer.base import WriterConfig, base
from pyspark.sql.streaming import StreamingQuery
from metadata.logger import ETLLogger
from delta.tables import DeltaTable
from transform.common.writer.batch_writer import DeltaBatchWriter

logger = ETLLogger.get_logger()


class DeltaBatchMergeWriter(base):
    def _write(
        self,
        writerConfig: WriterConfig,
    ):
        self.validate_required(writerConfig, "sparkSession", "primaryKeys")
        if DeltaTable.isDeltaTable(writerConfig.sparkSession, writerConfig.deltaPath):
            logger.info(
                f"Starting Delta Batch Merge to path '{writerConfig.deltaPath}'."
            )
            delta_table = DeltaTable.forPath(
                writerConfig.sparkSession, writerConfig.deltaPath
            )
            conditions = [
                f"target.{pk} = source.{pk}" for pk in writerConfig.primaryKeys
            ]
            if writerConfig.partitionCols:
                conditions.extend(
                    [
                        f"target.{col} = source.{col}"
                        for col in writerConfig.partitionCols
                    ]
                )

            join_condition = " AND ".join(conditions)

            # if data already exits in delta lake this command will overwrite match data by the newest
            delta_table.alias("target").merge(
                writerConfig.df.alias("source"), join_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            DeltaBatchWriter._write(writerConfig)
