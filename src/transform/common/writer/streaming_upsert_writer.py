from transform.common.writer.base import WriterConfig, base
from pyspark.sql.streaming import StreamingQuery
from metadata.logger import ETLLogger
from pyspark.sql import DataFrame, Window
from delta.tables import DeltaTable
from pyspark.sql.functions import row_number, col
from transform.common.writer.batch_writer import DeltaBatchWriter

logger = ETLLogger.get_logger()


class DeltaStreamUpsertWriter(base):
    def _write(
        self,
        writerConfig: WriterConfig,
    ) -> StreamingQuery:
        self.validate_required(
            writerConfig, "checkPointPath", "sparkSession", "primaryKeys"
        )
        logger.info(
            f"Starting Delta Stream Upsert Writer to path '{writerConfig.deltaPath}' with checkpoint at '{writerConfig.checkPointPath}'."
        )

        def upsert_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return

            # Take the latest record for each primary key based on kafka_timestamp
            # This is to ensure that if there are multiple records for the same primary key in the batch, only the latest one is considered for merging into the Delta table.
            # This also helps eliminate the need to use dropDuplicateWithWaterMark.
            w = Window.partitionBy(*writerConfig.primaryKeys).orderBy(
                col("kafka_timestamp").desc()
            )
            batch_df = (
                batch_df.withColumn("_rn", row_number().over(w))
                .filter("_rn = 1")
                .drop("_rn")
            )

            if DeltaTable.isDeltaTable(
                writerConfig.sparkSession, writerConfig.deltaPath
            ):
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
                delta_table.alias("target").merge(
                    batch_df.alias("source"), join_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            else:
                DeltaBatchWriter.write(
                    df=batch_df,
                    deltaPath=writerConfig.deltaPath,
                    partitionCols=writerConfig.partitionCols,
                )

        writer = (
            writerConfig.df.writeStream.foreachBatch(upsert_batch)
            .option("checkpointLocation", writerConfig.checkPointPath)
            .trigger(availableNow=True)
            .start()
        )
        return writer
