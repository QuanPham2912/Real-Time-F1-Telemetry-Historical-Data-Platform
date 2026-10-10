from unittest.mock import Mock, call

import pytest

import load.supabase as supabase_module
from load.supabase import GOLD_TABLES, Supabase


@pytest.fixture
def configure_supabase_env(monkeypatch):
    monkeypatch.setattr(supabase_module, "load_dotenv", Mock())
    for name, value in {
        "SUPABASE_DB_HOST": "db.example.test",
        "SUPABASE_DB_NAME": "f1",
        "SUPABASE_DB_USER": "f1_user",
        "SUPABASE_DB_PASSWORD": "secret",
        "SUPABASE_DB_PORT": "6543",
    }.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def spark():
    return Mock()


@pytest.fixture
def supabase(spark, configure_supabase_env):
    return Supabase(
        spark,
        goldBasePath="s3a://gold/",
        maxConnection=3,
        batchSize=2500,
    )


def test_init_reads_connection_environment_and_sets_jdbc_properties(supabase):
    assert supabase.jdbc_url.startswith(
        "jdbc:postgresql://db.example.test:6543/f1?user=f1_user"
    )
    assert supabase.connectionProperties == {
        "user": "f1_user",
        "password": "secret",
        "driver": "org.postgresql.Driver",
        "ssl": "true",
        "sslmode": "require",
        "reWriteBatchedInserts": "true",
    }
    supabase_module.load_dotenv.assert_called_once_with()


def test_init_uses_default_database_port(monkeypatch, spark, configure_supabase_env):
    monkeypatch.delenv("SUPABASE_DB_PORT")

    instance = Supabase(spark)

    assert instance.DB_PORT == "5432"


def test_init_rejects_missing_required_environment(monkeypatch, spark, configure_supabase_env):
    monkeypatch.delenv("SUPABASE_DB_PASSWORD")
    logger = Mock()
    monkeypatch.setattr(supabase_module, "logger", logger)

    with pytest.raises(ValueError):
        Supabase(spark)

    logger.error.assert_called_once_with(
        "Missing Supabase env vars: SUPABASE_DB_PASSWORD"
    )


def test_read_gold_source_loads_delta_from_gold_path(supabase, spark):
    expected_dataframe = Mock()
    spark.read.format.return_value.load.return_value = expected_dataframe

    result = supabase.read_gold_source("DIM_DRIVER")

    assert result is expected_dataframe
    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.load.assert_called_once_with(
        "s3a://gold/DIM_DRIVER"
    )


def test_count_in_postgres_executes_count_query_and_returns_integer(supabase, spark):
    jdbc_dataframe = Mock()
    jdbc_dataframe.collect.return_value = [{"n": "17"}]
    spark.read.jdbc.return_value = jdbc_dataframe

    result = supabase.count_in_postgres("fact_lap")

    assert result == 17
    spark.read.jdbc.assert_called_once_with(
        url=supabase.jdbc_url,
        table="(SELECT COUNT(*) AS n FROM fact_lap) AS t",
        properties=supabase.connectionProperties,
    )


def test_load_to_supabase_writes_and_verifies_row_count(monkeypatch, supabase):
    dataframe = Mock()
    dataframe.count.return_value = 17
    writer = Mock()
    writer.mode.return_value = writer
    writer.option.return_value = writer
    dataframe.write = writer
    monkeypatch.setattr(supabase, "read_gold_source", Mock(return_value=dataframe))
    count_in_postgres = Mock(return_value=17)
    monkeypatch.setattr(supabase, "count_in_postgres", count_in_postgres)
    logger = Mock()
    monkeypatch.setattr(supabase_module, "logger", logger)

    supabase.load_to_supabase("FACT_LAP")

    supabase.read_gold_source.assert_called_once_with("FACT_LAP")
    count_in_postgres.assert_called_once_with("fact_lap")
    assert writer.method_calls == [
        call.mode("overwrite"),
        call.option("truncate", "true"),
        call.option("batchSize", 2500),
        call.option("numPartitions", 3),
        call.jdbc(
            url=supabase.jdbc_url,
            table="fact_lap",
            properties=supabase.connectionProperties,
        ),
    ]
    logger.info.assert_any_call("loaded fact_lap: 17 rows (verified).")


def test_load_to_supabase_raises_on_row_count_mismatch(monkeypatch, supabase):
    dataframe = Mock()
    dataframe.count.return_value = 10
    writer = Mock()
    writer.mode.return_value = writer
    writer.option.return_value = writer
    dataframe.write = writer
    monkeypatch.setattr(supabase, "read_gold_source", Mock(return_value=dataframe))
    monkeypatch.setattr(supabase, "count_in_postgres", Mock(return_value=9))

    with pytest.raises(
        RuntimeError,
        match=r"Row count mismatch for dim_session: Gold=10, Supabase=9",
    ):
        supabase.load_to_supabase("DIM_SESSION")


def test_load_to_supabase_skips_row_count_verification_when_disabled(
    monkeypatch, supabase
):
    dataframe = Mock()
    writer = Mock()
    writer.mode.return_value = writer
    writer.option.return_value = writer
    dataframe.write = writer
    monkeypatch.setattr(supabase, "read_gold_source", Mock(return_value=dataframe))
    count_in_postgres = Mock()
    monkeypatch.setattr(supabase, "count_in_postgres", count_in_postgres)

    supabase.load_to_supabase("FACT_WEATHER", varify=False)

    dataframe.count.assert_not_called()
    count_in_postgres.assert_not_called()
    writer.jdbc.assert_called_once()


def test_load_to_supabase_rejects_unknown_gold_table(supabase):
    with pytest.raises(ValueError, match="Unknown Gold table 'UNKNOWN'"):
        supabase.load_to_supabase("UNKNOWN")


def test_load_all_to_supabase_loads_every_gold_table(monkeypatch, supabase):
    load_to_supabase = Mock()
    monkeypatch.setattr(supabase, "load_to_supabase", load_to_supabase)

    supabase.load_all_to_supabase(varify=False)

    assert load_to_supabase.call_args_list == [
        call(target_table=table, varify=False) for table in GOLD_TABLES
    ]


def test_load_all_to_supabase_raises_when_any_table_fails(monkeypatch, supabase):
    load_to_supabase = Mock()
    load_to_supabase.side_effect = [None, RuntimeError("load failed")] + [None] * 6
    monkeypatch.setattr(supabase, "load_to_supabase", load_to_supabase)
    logger = Mock()
    monkeypatch.setattr(supabase_module, "logger", logger)

    with pytest.raises(RuntimeError):
        supabase.load_all_to_supabase()

    assert load_to_supabase.call_count == len(GOLD_TABLES)
    logger.error.assert_any_call("Failed to load DIM_DRIVER : load failed")
    logger.error.assert_any_call("Supabase load failed for: DIM_DRIVER")