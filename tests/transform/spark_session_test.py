from types import SimpleNamespace
from unittest.mock import Mock

import transform.spark_session as spark_module


class FakeBuilder:
    def __init__(self, session):
        self.session = session
        self.calls = []
        self.get_getOrCreate = Mock(side_effect=AssertionError("legacy typo method should not be called"))

    def appName(self, value):
        self.calls.append(("appName", value))
        return self

    def master(self, value):
        self.calls.append(("master", value))
        return self

    def config(self, key, value):
        self.calls.append(("config", key, value))
        return self

    def getOrCreate(self):
        self.calls.append(("getOrCreate",))
        return self.session


def test_get_session_uses_default_values_and_builds_session(monkeypatch):
    logger = Mock()
    session = object()
    builder = FakeBuilder(session)
    fake_session = SimpleNamespace(builder=builder)

    monkeypatch.setattr(spark_module, "logger", logger)
    monkeypatch.setattr(spark_module, "SparkSession", fake_session)

    result = spark_module.F1SparkSession.get_session()

    assert result is session
    logger.info.assert_called_once_with(
        "Initializing SparkSession with app name 'F1_ETL_App' and master 'local[*]'."
    )
    assert builder.calls == [
        ("appName", "F1_ETL_App"),
        ("master", "local[*]"),
        (
            "config",
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0",
        ),
        ("config", "spark.sql.shuffle.partitions", "4"),
        ("getOrCreate",),
    ]


def test_get_session_uses_custom_values(monkeypatch):
    logger = Mock()
    session = object()
    builder = FakeBuilder(session)
    fake_session = SimpleNamespace(builder=builder)

    monkeypatch.setattr(spark_module, "logger", logger)
    monkeypatch.setattr(spark_module, "SparkSession", fake_session)

    result = spark_module.F1SparkSession.get_session(app_name="CustomApp", master="local[2]")

    assert result is session
    logger.info.assert_called_once_with(
        "Initializing SparkSession with app name 'CustomApp' and master 'local[2]'."
    )
    assert builder.calls[0] == ("appName", "CustomApp")
    assert builder.calls[1] == ("master", "local[2]")
