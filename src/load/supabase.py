import os
import posixpath
 
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
 
from metadata.logger import ETLLogger
 
logger = ETLLogger.get_logger()

GOLD_TABLES = [
    "DIM_SESSION",
    "DIM_DRIVER",
    "DIM_CONSTRUCTOR",
    "DIM_ENGINE_SUPPLIER",
    "FACT_RESULT",
    "FACT_LAP",
    "FACT_TELEMETRY",
    "FACT_WEATHER",
    ]

class Supabase:
    def __init__(self,
                 sparkSession :SparkSession,
                 goldBasePath :str = "s3a://f1-gold/",
                 maxConnection :int = 4,
                 batchSize : int = 10000):
        self.sparkSession = sparkSession
        self.goldBasePath = goldBasePath
        self.maxConnection = maxConnection
        self.batchSize = batchSize

        load_dotenv()
        self.DB_HOST = os.getenv("SUPABASE_DB_HOST")
        self.DB_NAME = os.getenv("SUPABASE_DB_NAME")
        self.DB_USER = os.getenv("SUPABASE_DB_USER")
        self.DB_PASSWORD = os.getenv("SUPABASE_DB_PASSWORD")
        self.DB_PORT = os.getenv("SUPABASE_DB_PORT", "5432")

        missing = [name for name, value in {
            "SUPABASE_DB_HOST": self.DB_HOST,
            "SUPABASE_DB_NAME": self.DB_NAME,
            "SUPABASE_DB_USER": self.DB_USER,
            "SUPABASE_DB_PASSWORD": self.DB_PASSWORD,
        }.items() if not value]
        if missing:
            logger.error(f"Missing Supabase env vars: {', '.join(missing)}")
            raise ValueError

        self.jdbc_url = f"jdbc:postgresql://{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}?user={self.DB_USER}&password={self.DB_PASSWORD}"
        self.connectionProperties = {
            "user" :self.DB_USER,
            "password" : self.DB_PASSWORD,
            "driver" : "org.postgresql.Driver",
            "ssl" : "true",
            "sslmode" : "require",
            "reWriteBatchedInserts" : "true",
        }
        

    def read_gold_source(self, gold_source : str) -> DataFrame:
        gold_path = posixpath.join(self.goldBasePath, gold_source)
        gold_df = self.sparkSession.read.format("delta").load(gold_path)
        return gold_df

    def count_in_postgres(self, table:str) -> int:
        query = f"(SELECT COUNT(*) AS n FROM {table}) AS t"
        row = self.sparkSession.read.jdbc(
            url = self.jdbc_url,
            table = query,
            properties = self.connectionProperties
        ).collect()[0]
        return int(row["n"])

    def load_to_supabase(self ,
                         target_table :str,
                         varify :bool = True):
        table = target_table.upper()
        if table not in GOLD_TABLES:
            raise ValueError(f"Unknown Gold table '{target_table}'. Expected one of {GOLD_TABLES}")

        pg_table = target_table.lower()
        df = self.read_gold_source(target_table)
        expected = df.count() if varify else None

        logger.info(f"Loading '{table}' to supabase table: '{pg_table}'")

        df.write \
            .mode("overwrite") \
            .option("truncate", "true") \
            .option("batchSize", self.batchSize) \
            .option("numPartitions", self.maxConnection) \
            .jdbc(
                url = self.jdbc_url,
                table = pg_table,
                properties = self.connectionProperties
            )

        if varify:
            actual = self.count_in_postgres(pg_table)
            if actual != expected:
                raise RuntimeError(f"Row count mismatch for {pg_table}: Gold={expected}, Supabase={actual}")

            logger.info(f"loaded {pg_table}: {actual} rows (verified).")

    def load_all_to_supabase(self,
                             varify :bool = True):
        failed = []
        for table in GOLD_TABLES:
            try:
                self.load_to_supabase(target_table = table,
                                      varify = varify)
            except Exception as e:
                logger.error(f"Failed to load {table} : {e}")
                failed.append(table)
        if failed:
            logger.error(f"Supabase load failed for: {', '.join(failed)}")
            raise RuntimeError
