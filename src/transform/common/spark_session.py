from pyspark.sql import SparkSession
from metadata.logger import ETLLogger
from metadata.setting import get_settings

logger = ETLLogger.get_logger()
settings = get_settings()
s3 = settings.s3()


class F1SparkSession:
    @staticmethod
    def get_session(
        app_name: str = "F1_ETL_App", master: str | None = None
    ) -> SparkSession:
        master = master or settings.spark_master
        s3_endpoint = s3.endpoint
        s3_access_key = s3.access_key
        s3_secret_key = s3.secret_key

        if not s3_access_key or not s3_secret_key or not s3_endpoint:
            logger.error("Missing S3 connection info")
            raise ValueError
        logger.info(
            f"Initializing SparkSession with app name '{app_name}' and master '{master}'."
        )
        return (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.3",
            )
            # Required configuration for full Delta Lake functionality
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # Local resource limits (Airflow task + Spark share one container)
            .config("spark.driver.memory", settings.spark_driver_memory)
            .config("spark.sql.shuffle.partitions", settings.spark_shuffle_partitions)
            .config("spark.ui.enabled", settings.spark_ui_enabled)
            # S3-compatible storage (RustFS)
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(s3_endpoint.startswith("https")).lower(),
            )
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            # Time zone
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
