from pyspark.sql import SparkSession
from metadata.logger import ETLLogger
from dotenv import load_dotenv
import os

logger = ETLLogger.get_logger()

class F1SparkSession:
    @staticmethod
    def get_session(app_name: str = "F1_ETL_App", master: str | None = None) -> SparkSession:
        load_dotenv()
        master = master or os.getenv("SPARK_MASTER", "local[*]")
        s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
        s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin123")

        if not s3_access_key or not s3_secret_key or not s3_endpoint:
            logger.error("Missing MINIO connection info")
            raise ValueError
        logger.info(f"Initializing SparkSession with app name '{app_name}' and master '{master}'.")
        return (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.3"
            )
            # Required configuration for full Delta Lake functionality
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

            # Local resource limits (Airflow task + Spark share one container)
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
            .config("spark.ui.enabled", os.getenv("SPARK_UI_ENABLED", "false"))

            # MinIO Connection
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)  # Dùng "http://minio:9000" nếu chạy TRONG Docker
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(s3_endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            #Time zone
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )