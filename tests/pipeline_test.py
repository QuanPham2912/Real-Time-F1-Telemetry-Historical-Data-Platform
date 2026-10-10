import sys
import types
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import Mock, call

import pandas as pd
import pytest

import pipeline as pipeline_module
from metadata.f1_topic import F1Topic


def fake_module(monkeypatch, name, **attributes):
    module = types.ModuleType(name)
    for attribute, value in attributes.items():
        setattr(module, attribute, value)
    monkeypatch.setitem(sys.modules, name, module)
    return module


def install_extract_dependencies(monkeypatch):
    jolpica = Mock()
    statsf1 = Mock()
    fastf1 = Mock()
    producer = Mock()
    topic_manager = Mock()
    topic_manager.__enter__ = Mock(return_value=topic_manager)
    topic_manager.__exit__ = Mock(return_value=False)

    fake_module(monkeypatch, "extract.api_client", JolicaClient=Mock(return_value=jolpica))
    fake_module(
        monkeypatch, "extract.fastf1_extractor", FastF1Extractor=Mock(return_value=fastf1)
    )
    fake_module(monkeypatch, "extract.scraper", StatsF1=Mock(return_value=statsf1))
    fake_module(
        monkeypatch,
        "streaming.producer",
        KafkaProducer=Mock(return_value=producer),
    )
    topic_manager_class = Mock(return_value=topic_manager)
    fake_module(
        monkeypatch,
        "streaming.topic_manager",
        KafkaTopicManager=topic_manager_class,
    )
    return jolpica, statsf1, fastf1, producer, topic_manager, topic_manager_class


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, []), ({"race": 1}, [{"race": 1}]), ([{"race": 1}], [{"race": 1}])],
)
def test_as_records(value, expected):
    assert pipeline_module.as_records(value) == expected


def test_as_records_preserves_generators():
    records = (record for record in ({"race": 1},))
    assert pipeline_module.as_records(records) is records


def test_resolve_rounds_returns_numeric_round_without_loading_schedule():
    assert pipeline_module.resolve_rounds(2024, "7") == [7]


@pytest.mark.parametrize(
    ("round_arg", "expected"),
    [("all", [1, 2]), ("latest", [2]), ("LATEST", [2])],
)
def test_resolve_rounds_uses_only_finished_events(monkeypatch, round_arg, expected):
    today = datetime.now(timezone.utc).date()
    schedule = pd.DataFrame(
        {
            "EventDate": [
                today - pd.Timedelta(2, unit="D"),
                today - pd.Timedelta(1, unit="D"),
                today + pd.Timedelta(1, unit="D"),
            ],
            "RoundNumber": [1, 2, 3],
        }
    )
    get_event_schedule = Mock(return_value=schedule)
    fake_module(monkeypatch, "fastf1", get_event_schedule=get_event_schedule)

    assert pipeline_module.resolve_rounds(2026, round_arg) == expected
    get_event_schedule.assert_called_once_with(2026, include_testing=False)


def test_resolve_rounds_rejects_unsupported_round(monkeypatch):
    today = datetime.now(timezone.utc).date()
    schedule = pd.DataFrame(
        {"EventDate": [today - pd.Timedelta(1, unit="D")], "RoundNumber": [1]}
    )
    fake_module(monkeypatch, "fastf1", get_event_schedule=Mock(return_value=schedule))
    logger = Mock()
    monkeypatch.setattr(pipeline_module, "logger", logger)

    with pytest.raises(ValueError):
        pipeline_module.resolve_rounds(2026, "next")

    logger.error.assert_called_once_with(
        "--round must be a number, 'all' or 'latest' (got 'next')"
    )


def test_season_converts_current_to_current_utc_year(monkeypatch):
    fixed_now = datetime(2030, 1, 1, tzinfo=timezone.utc)
    fake_datetime = Mock()
    fake_datetime.now.return_value = fixed_now
    monkeypatch.setattr(pipeline_module, "datetime", fake_datetime)

    assert pipeline_module._season("current") == 2030
    fake_datetime.now.assert_called_once_with(timezone.utc)


def test_season_converts_explicit_year():
    assert pipeline_module._season("2024") == 2024


def test_tables_normalizes_and_splits_table_names():
    args = types.SimpleNamespace(tables=" dim_driver,FACT_LAP , dim_race ")

    assert pipeline_module._tables(args) == {"DIM_DRIVER", "FACT_LAP", "DIM_RACE"}


@pytest.mark.parametrize("args", [types.SimpleNamespace(), types.SimpleNamespace(tables=None)])
def test_tables_returns_none_when_not_selected(args):
    assert pipeline_module._tables(args) is None


def test_build_parser_parses_stage_defaults_and_options():
    parser = pipeline_module.build_parser()

    extract = parser.parse_args(["extract"])
    assert extract.stage == "extract"
    assert extract.season == "current"
    assert extract.to_season is None
    assert extract.round == "latest"
    assert extract.source == "jolpica,statsf1,fastf1"
    assert extract.sessions == "R"
    assert extract.allow_partial is False
    assert extract.sleep == 1.0

    configured = parser.parse_args(
        [
            "all",
            "--season",
            "2022",
            "--to-season",
            "2024",
            "--round",
            "all",
            "--source",
            "fastf1",
            "--sessions",
            "Q,R",
            "--allow-partial",
            "--sleep",
            "0",
        ]
    )
    assert configured.stage == "all"
    assert configured.season == "2022"
    assert configured.to_season == "2024"
    assert configured.round == "all"
    assert configured.source == "fastf1"
    assert configured.sessions == "Q,R"
    assert configured.allow_partial is True
    assert configured.sleep == 0

    for stage in ("silver", "gold", "load"):
        stage_args = parser.parse_args([stage, "--tables", "dim_driver,FACT_LAP"])
        assert stage_args.stage == stage
        assert stage_args.tables == "dim_driver,FACT_LAP"


def test_run_extract_initializes_sources_publishes_records_and_closes(monkeypatch):
    jolpica, statsf1, fastf1, producer, topic_manager, topic_manager_class = (
        install_extract_dependencies(monkeypatch)
    )
    jolpica.run.return_value = {
        "Race": {"round": "1"},
        "Driver": None,
        "Constructor": [],
        "Results": [{"position": "1"}],
    }
    statsf1.run.return_value = {
        "Driver": [{"driverId": "driver"}],
        "Result": [{"position": "2"}],
    }
    fastf1.run.return_value = {
        "lap_data": [{"lap": 1}],
        "weather_data": None,
        "telemetry_stream": [{"speed": 300}],
    }
    args = pipeline_module.build_parser().parse_args(
        [
            "extract",
            "--season",
            "2024",
            "--round",
            "1",
            "--sessions",
            "Q,R",
            "--sleep",
            "0",
        ]
    )
    monkeypatch.setattr(pipeline_module, "_season", lambda value: int(value))
    monkeypatch.setattr(pipeline_module, "resolve_rounds", Mock(return_value=[1]))
    sleep = Mock()
    monkeypatch.setattr(pipeline_module.time, "sleep", sleep)

    pipeline_module.run_extract(args)

    topic_manager_class.assert_called_once_with(pipeline_module.KAFKA)
    topic_manager.init_topics.assert_called_once_with()
    jolpica.run.assert_called_once_with(season=2024, round=1)
    statsf1.run.assert_called_once_with(season=2024, round=1)
    assert fastf1.run.call_args_list == [
        call(season=2024, round=1, session_type="Q"),
        call(season=2024, round=1, session_type="R"),
    ]
    assert producer.send_many.call_args_list == [
        call(topic_name=F1Topic.JOLPICA_RACE, messages=[{"round": "1"}]),
        call(topic_name=F1Topic.JOLPICA_DRIVER, messages=[]),
        call(topic_name=F1Topic.JOLPICA_CONSTRUCTOR, messages=[]),
        call(topic_name=F1Topic.JOLPICA_RACE_RESULT, messages=[{"position": "1"}]),
        call(topic_name=F1Topic.STATSF1_DRIVER_STATSF1, messages=[{"driverId": "driver"}]),
        call(topic_name=F1Topic.STATSF1_CONSTRUCTOR_STATSF1, messages=[]),
        call(topic_name=F1Topic.STATSF1_ENGINE_SUPPLIER_STATSF1, messages=[]),
        call(topic_name=F1Topic.STATSF1_CAR_STATSF1, messages=[]),
        call(topic_name=F1Topic.STATSF1_RACE_RESULT, messages=[{"position": "2"}]),
        call(topic_name=F1Topic.FASTF1_LAP, messages=[{"lap": 1}]),
        call(topic_name=F1Topic.FASTF1_WEATHER, messages=[]),
        call(topic_name=F1Topic.FASTF1_TELEMETRY, messages=[{"speed": 300}]),
        call(topic_name=F1Topic.FASTF1_LAP, messages=[{"lap": 1}]),
        call(topic_name=F1Topic.FASTF1_WEATHER, messages=[]),
        call(topic_name=F1Topic.FASTF1_TELEMETRY, messages=[{"speed": 300}]),
    ]
    assert producer.flush.call_count == 4
    producer.close.assert_called_once_with()
    sleep.assert_called_once_with(0.0)


def test_run_extract_normalizes_source_names_and_season_range(monkeypatch):
    jolpica, statsf1, fastf1, producer, _, _ = install_extract_dependencies(monkeypatch)
    jolpica.run.return_value = {}
    seasons_and_rounds = []
    monkeypatch.setattr(pipeline_module, "_season", lambda value: int(value))
    monkeypatch.setattr(
        pipeline_module,
        "resolve_rounds",
        lambda season, round_arg: seasons_and_rounds.append((season, round_arg)) or [3],
    )
    monkeypatch.setattr(pipeline_module.time, "sleep", Mock())
    args = pipeline_module.build_parser().parse_args(
        [
            "extract",
            "--season",
            "2022",
            "--to-season",
            "2023",
            "--round",
            "latest",
            "--source",
            " JOLPICA ",
        ]
    )

    pipeline_module.run_extract(args)

    assert seasons_and_rounds == [(2022, "latest"), (2023, "latest")]
    assert jolpica.run.call_args_list == [
        call(season=2022, round=3),
        call(season=2023, round=3),
    ]
    statsf1.run.assert_not_called()
    fastf1.run.assert_not_called()
    producer.close.assert_called_once_with()


def test_run_extract_rejects_unknown_source(monkeypatch):
    *_, topic_manager_class = install_extract_dependencies(monkeypatch)
    logger = Mock()
    monkeypatch.setattr(pipeline_module, "logger", logger)
    args = pipeline_module.build_parser().parse_args(
        ["extract", "--source", "jolpica,unknown"]
    )

    with pytest.raises(ValueError):
        pipeline_module.run_extract(args)

    logger.error.assert_called_once()
    topic_manager_class.assert_not_called()


@pytest.mark.parametrize("allow_partial", [False, True])
def test_run_extract_handles_job_failures_and_always_closes(
    monkeypatch, allow_partial
):
    jolpica, _, _, producer, _, _ = install_extract_dependencies(monkeypatch)
    jolpica.run.side_effect = RuntimeError("source unavailable")
    monkeypatch.setattr(pipeline_module, "_season", lambda value: 2024)
    monkeypatch.setattr(pipeline_module, "resolve_rounds", lambda *_: [2])
    monkeypatch.setattr(pipeline_module.time, "sleep", Mock())
    logger = Mock()
    monkeypatch.setattr(pipeline_module, "logger", logger)
    args = pipeline_module.build_parser().parse_args(
        ["extract", "--source", "jolpica", "--allow-partial"] if allow_partial
        else ["extract", "--source", "jolpica"]
    )

    if allow_partial:
        assert pipeline_module.run_extract(args) is None
        logger.warning.assert_called_once_with(
            "1 extract job failed at : ['jolpica:2024:2'] "
            "(continuing because --allow-partial)"
        )
    else:
        with pytest.raises(
            RuntimeError,
            match=r"1 extract job failed at : \['jolpica:2024:2'\]",
        ):
            pipeline_module.run_extract(args)
        logger.warning.assert_not_called()
    logger.exception.assert_called_once_with(
        "Extract failed: jolpica season=2024 round=2"
    )
    producer.close.assert_called_once_with()


def test_spark_session_yields_spark_and_stops_it(monkeypatch):
    spark = Mock()
    session_class = Mock()
    session_class.get_session.return_value = spark
    fake_module(
        monkeypatch,
        "transform.common.spark_session",
        F1SparkSession=session_class,
    )

    with pipeline_module.spark_session() as result:
        assert result is spark

    session_class.get_session.assert_called_once_with()
    spark.stop.assert_called_once_with()


def test_spark_session_stops_spark_when_stage_raises(monkeypatch):
    spark = Mock()
    session_class = Mock()
    session_class.get_session.return_value = spark
    fake_module(
        monkeypatch,
        "transform.common.spark_session",
        F1SparkSession=session_class,
    )

    with pytest.raises(RuntimeError, match="stage failed"):
        with pipeline_module.spark_session():
            raise RuntimeError("stage failed")

    spark.stop.assert_called_once_with()


def test_run_bronze_processes_all_topics_in_order(monkeypatch):
    queries = [Mock() for _ in pipeline_module.BRONZE_ORDER]
    job = Mock()
    job.write_to_bronze.side_effect = queries
    bronze_job_class = Mock(return_value=job)
    fake_module(
        monkeypatch,
        "transform.bronze.bronze_job",
        BronzeJob=bronze_job_class,
    )
    spark = Mock()

    pipeline_module.run_bronze(spark)

    bronze_job_class.assert_called_once_with(
        sparkSession=spark, bootstrapServer=pipeline_module.KAFKA
    )
    assert job.write_to_bronze.call_args_list == [
        call(topic) for topic in pipeline_module.BRONZE_ORDER
    ]
    for query in queries:
        query.awaitTermination.assert_called_once_with()
    assert pipeline_module.BRONZE_ORDER[-1] is F1Topic.FASTF1_TELEMETRY


def test_run_silver_filters_tables_and_waits_for_stream(monkeypatch):
    base_config = object()
    derived_config = object()
    query = Mock()
    job = Mock()
    job.process_base_table.return_value = query
    job.process_derived_table.return_value = None
    silver_job_class = Mock(return_value=job)
    fake_module(monkeypatch, "transform.silver.silver_job", silverJob=silver_job_class)
    fake_module(
        monkeypatch,
        "transform.silver.silver_table_config",
        BASE_TABLE_CONFIGS={"FACT_LAP": base_config},
        DERIVED_TABLE_CONFIGS={"FACT_WEATHER": derived_config},
    )

    pipeline_module.run_silver(Mock(), only={"FACT_LAP", "FACT_WEATHER"})

    assert job.process_base_table.call_args_list == [call(base_config)]
    assert job.process_derived_table.call_args_list == [call(derived_config)]
    query.awaitTermination.assert_called_once_with()


def test_run_silver_raises_for_table_missing_from_configs(monkeypatch):
    fake_module(monkeypatch, "transform.silver.silver_job", silverJob=Mock())
    fake_module(
        monkeypatch,
        "transform.silver.silver_table_config",
        BASE_TABLE_CONFIGS={},
        DERIVED_TABLE_CONFIGS={},
    )

    with pytest.raises(KeyError, match="DIM_DRIVER.*not in any silver config"):
        pipeline_module.run_silver(Mock(), only={"DIM_DRIVER"})


def test_run_gold_filters_tables_and_preserves_config_order(monkeypatch):
    first_config = object()
    last_config = object()
    job = Mock()
    gold_job_class = Mock(return_value=job)
    fake_module(monkeypatch, "transform.gold.gold_job", goldJob=gold_job_class)
    fake_module(
        monkeypatch,
        "transform.gold.gold_table_config",
        GOLD_TABLE_CONFIGS={
            "FACT_LAP": first_config,
            "FACT_TELEMETRY": last_config,
        },
    )

    pipeline_module.run_gold(Mock(), only={"FACT_TELEMETRY", "FACT_LAP"})

    assert job.process_gold_table.call_args_list == [
        call(first_config),
        call(last_config),
    ]


def test_run_load_loads_selected_tables_or_all(monkeypatch):
    database = Mock()
    supabase_class = Mock(return_value=database)
    fake_module(monkeypatch, "load.supabase", Supabase=supabase_class)
    spark = Mock()

    pipeline_module.run_load(spark, only=["DIM_DRIVER", "FACT_LAP"])
    pipeline_module.run_load(spark)

    assert supabase_class.call_args_list == [
        call(sparkSession=spark),
        call(sparkSession=spark),
    ]
    assert database.load_to_supabase.call_args_list == [
        call("DIM_DRIVER"),
        call("FACT_LAP"),
    ]
    database.load_all_to_supabase.assert_called_once_with(varify=True)


def test_main_dispatches_simple_stage_inside_spark_context(monkeypatch):
    spark = Mock()
    spark_context = Mock()
    spark_context.__enter__ = Mock(return_value=spark)
    spark_context.__exit__ = Mock(return_value=False)
    spark_session = Mock(return_value=spark_context)
    run_silver = Mock()
    logger = Mock()
    monkeypatch.setattr(pipeline_module, "spark_session", spark_session)
    monkeypatch.setattr(pipeline_module, "run_silver", run_silver)
    monkeypatch.setattr(pipeline_module, "logger", logger)

    result = pipeline_module.main(["silver", "--tables", "dim_driver,FACT_LAP"])

    assert result == 0
    spark_session.assert_called_once_with()
    run_silver.assert_called_once_with(spark, {"DIM_DRIVER", "FACT_LAP"})
    logger.info.assert_called_once_with("Stage 'silver' finished OK")
    logger.exception.assert_not_called()


def test_main_runs_all_stages_in_sequence(monkeypatch):
    spark = Mock()
    spark_context = Mock()
    spark_context.__enter__ = Mock(return_value=spark)
    spark_context.__exit__ = Mock(return_value=False)
    spark_session = Mock(return_value=spark_context)
    calls = []
    monkeypatch.setattr(pipeline_module, "spark_session", spark_session)
    monkeypatch.setattr(pipeline_module, "run_extract", lambda args: calls.append("extract"))
    monkeypatch.setattr(pipeline_module, "run_bronze", lambda value: calls.append("bronze"))
    monkeypatch.setattr(pipeline_module, "run_silver", lambda value: calls.append("silver"))
    monkeypatch.setattr(pipeline_module, "run_gold", lambda value: calls.append("gold"))
    monkeypatch.setattr(pipeline_module, "run_load", lambda value: calls.append("load"))
    monkeypatch.setattr(pipeline_module, "logger", Mock())

    assert pipeline_module.main(["all"]) == 0

    assert calls == ["extract", "bronze", "silver", "gold", "load"]


def test_main_returns_one_and_logs_stage_failure(monkeypatch):
    error = RuntimeError("silver failed")
    run_silver = Mock(side_effect=error)
    spark_context = Mock()
    spark_context.__enter__ = Mock(return_value=Mock())
    spark_context.__exit__ = Mock(return_value=False)
    logger = Mock()
    monkeypatch.setattr(pipeline_module, "spark_session", Mock(return_value=spark_context))
    monkeypatch.setattr(pipeline_module, "run_silver", run_silver)
    monkeypatch.setattr(pipeline_module, "logger", logger)

    assert pipeline_module.main(["silver"]) == 1

    logger.exception.assert_called_once_with("Stage 'silver' failed")
    logger.info.assert_not_called()


def test_main_parser_requires_stage():
    with pytest.raises(SystemExit) as exc_info:
        pipeline_module.main([])

    assert exc_info.value.code == 2