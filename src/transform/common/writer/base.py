from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.streaming import StreamingQuery
from delta.tables import DeltaTable
from metadata.logger import ETLLogger
from pyspark.sql.functions import row_number, col
from abc import ABC, abstractmethod
from dataclasses import dataclass

logger = ETLLogger.get_logger()


@dataclass
class WriterConfig:
    df: DataFrame
    deltaPath: str
    outputMode: str = "append"
    checkPointPath: str | None = None
    sparkSession: SparkSession | None = None
    partitionCols: list[str] | None = None
    primaryKeys: list[str] | None = None
    replaceConditionCols: list[str] | None = None


class base(ABC):
    def write(self, **kwargs):
        return self._write(WriterConfig(**kwargs))
    
    @abstractmethod
    def _write(
        self,
        writerConfig: WriterConfig,
    ):
        pass

    def validate_required(self, config: WriterConfig, *required_field):
        for field in required_field:
            if getattr(config, field) is None:
                raise ValueError(f"{field} must be provided for the writer.")
