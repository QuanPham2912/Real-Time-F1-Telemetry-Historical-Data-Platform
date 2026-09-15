from pyspark.sql import SparkSession
from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()

class F1SparkSession:
    @staticmethod
    def get_session(app_name: str = "F1_ETL_App", master: str = "local[*]") -> SparkSession:
        logger.info(f"Initializing SparkSession with app name '{app_name}' and master '{master}'.")
        return (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Allow kafka connection
            .config("spark.sql.shuffle.partitions", "4")  # Reduce the number of shuffle partitions for local testing
            .getOrCreate()
        )