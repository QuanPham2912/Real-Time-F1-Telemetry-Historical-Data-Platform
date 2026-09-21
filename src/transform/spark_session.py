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
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.1.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            )
            # Required configuration for full Delta Lake functionality
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Optimization for local environment
            .config("spark.sql.shuffle.partitions", "4")
        # MinIO Connection
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")  # Dùng "http://minio:9000" nếu chạy TRONG Docker
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )