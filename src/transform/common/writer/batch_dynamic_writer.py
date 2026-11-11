from transform.common.writer.base import WriterConfig, base
from pyspark.sql.streaming import StreamingQuery
from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()


class DeltaBatchDynamicWriter(base):
    def build_replace_condition(self, df, partition_cols: list) -> str:
        conditions = []
        for col_name in partition_cols:
            # Ingest distinct value from partition cols
            distint_vals = [
                row[col_name] for row in df.select(col_name).distinct().collect()
            ]
            formatted_vals = [
                f"'{val}'" if isinstance(val, str) else str(val) for val in distint_vals
            ]

            if len(formatted_vals) > 0:
                conditions.append(f"{col_name} IN ({', '.join(formatted_vals)})")
        return " AND ".join(conditions)

    def _write(
        self,
        writerConfig: WriterConfig,
    ):
        logger.info(
            f"Starting Delta Dynamic Partition Overwrite to path '{writerConfig.deltaPath}'."
        )

        replaceCondition = (
            self.build_replace_condition(
                writerConfig.df, writerConfig.replaceConditionCols
            )
            if writerConfig.replaceConditionCols
            else None
        )
        writer = writerConfig.df.write.format("delta").mode("overwrite")
        if writerConfig.partitionCols:
            writer = writer.partitionBy(*writerConfig.partitionCols)
        if replaceCondition:
            writer = writer.option("replaceWhere", replaceCondition)

        writer.save(writerConfig.deltaPath)
