from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv


class ConfigError(RuntimeError):
    """Missing or wrong configuration."""


def require(*names: str) -> dict[str, str]:
    values = {n: os.getenv(n) for n in names}
    missing = [n for n, v in values.items() if not v]
    if missing:
        raise ConfigError(
            f"Missing required environment variables: {', '.join(missing)}"
        )
    return values


@dataclass(frozen=True)
class S3Settings:
    endpoint: str
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class JdbcSettings:
    host: str
    name: str
    user: str
    password: str
    port: str = "5432"


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    bronze_base_path: str
    silver_base_path: str
    gold_base_path: str
    spark_master: str
    spark_driver_memory: str
    spark_shuffle_partitions: str
    spark_ui_enabled: str
    s3_endpoint: str
    fastf1_cache_dir: str
    timezone: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092"
            ),
            bronze_base_path=os.getenv("BRONZE_BASE_PATH", "s3a://f1-bronze/"),
            silver_base_path=os.getenv("SILVER_BASE_PATH", "s3a://f1-silver/"),
            gold_base_path=os.getenv("GOLD_BASE_PATH", "s3a://f1-gold/"),
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            spark_shuffle_partitions=os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"),
            spark_ui_enabled=os.getenv("SPARK_UI_ENABLED", "false"),
            s3_endpoint=os.getenv("S3_ENDPOINT", "http://s3:9000"),
            fastf1_cache_dir=os.getenv("FASTF1_CACHE_DIR", "/tmp/fastf1_cache"),
            timezone=os.getenv("TIMEZONE", "UTC"),
        )

    # Secret settings that should not be exposed in the environment variables
    def s3(self) -> S3Settings:
        v = require("S3_ACCESS_KEY", "S3_SECRET_KEY")
        return S3Settings(self.s3_endpoint, v["S3_ACCESS_KEY"], v["S3_SECRET_KEY"])

    def supabase(self) -> JdbcSettings:
        v = require(
            "SUPABASE_DB_HOST",
            "SUPABASE_DB_NAME",
            "SUPABASE_DB_USER",
            "SUPABASE_DB_PASSWORD",
        )
        return JdbcSettings(
            v["SUPABASE_DB_HOST"],
            v["SUPABASE_DB_NAME"],
            v["SUPABASE_DB_USER"],
            v["SUPABASE_DB_PASSWORD"],
            os.getenv("SUPABASE_DB_PORT", "5432"),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()
    return Settings.from_env()
