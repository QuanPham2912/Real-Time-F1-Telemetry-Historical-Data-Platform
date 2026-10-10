import argparse
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
 
from metadata.f1_topic import F1Topic as T
from metadata.logger import ETLLogger
 
logger = ETLLogger.get_logger()
 
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092")

#EXECUTION PLAN
# extractor result key -> Kafka topic
JOLPICA_TOPICS = {
    "Race": T.JOLPICA_RACE,
    "Driver": T.JOLPICA_DRIVER,
    "Constructor": T.JOLPICA_CONSTRUCTOR,
    "Results": T.JOLPICA_RACE_RESULT,
}
STATSF1_TOPICS = {
    "Driver": T.STATSF1_DRIVER_STATSF1,
    "Constructor": T.STATSF1_CONSTRUCTOR_STATSF1,
    "Engine_Supplier": T.STATSF1_ENGINE_SUPPLIER_STATSF1,
    "Car": T.STATSF1_CAR_STATSF1,
    "Result": T.STATSF1_RACE_RESULT,
}
FASTF1_TOPICS = {
    "lap_data": T.FASTF1_LAP,
    "weather_data": T.FASTF1_WEATHER,
    "telemetry_stream": T.FASTF1_TELEMETRY,
}
 
# Bronze: every topic, the heaviest one (telemetry) last.
BRONZE_ORDER = [t for t in T if t != T.FASTF1_TELEMETRY] + [T.FASTF1_TELEMETRY]
 
# Silver: all of these read from Bronze, so they are independent of each other,
# EXCEPT XWALK_DRIVER which reads Silver XWALK_CONSTRUCTOR -> keep it after.
SILVER_ORDER = [
    # base, batch
    "DIM_DRIVER",
    "DIM_CONSTRUCTOR",
    "DIM_DRIVER_STATSF1",
    "DIM_ENGINE_SUPPLIER_STATSF1",
    "DIM_CAR_STATSF1",
    "DIM_CONSTRUCTOR_STATSF1",
    # derived
    "DIM_CIRCUIT",
    "DIM_RACE",
    "DIM_SESSION",
    "FACT_RESULT",
    "FACT_RESULTS_STATSF1",
    "XWALK_CONSTRUCTOR",
    "XWALK_DRIVER",  # needs XWALK_CONSTRUCTOR
    # base, streaming (run with availableNow, so they finish by themselves)
    "FACT_LAP",
    "FACT_WEATHER",
    "FACT_TELEMETRY",
]
 
# Gold: only real dependency is FACT_TELEMETRY -> Gold FACT_LAP.
GOLD_ORDER = [
    "DIM_SESSION",
    "DIM_DRIVER",
    "DIM_CONSTRUCTOR",
    "DIM_ENGINE_SUPPLIER",
    "FACT_RESULT",
    "FACT_LAP",
    "FACT_WEATHER",
    "FACT_TELEMETRY",  # must come after FACT_LAP
]


@contextmanager
def spark_session():
    """Initializing and providing a session (yield spark): When you call this function within a `with` block, it retrieves a `SparkSession` (via `F1SparkSession.get_session()`) and yields that Spark object for use inside the `with` block."""
    from transform.common.spark_session import F1SparkSession

    spark = F1SparkSession.get_session()
    try:
        yield spark
    finally:
        spark.stop()

def as_records(data):
    """Extractors return list[dict] (or a generator for telemetry); wrap a lone dict."""
    if data is None:
        return [] #Keep return a list to not impact the process
    if isinstance(data, dict):
        return [data]
    return data

def resolve_rounds(season: int, round_arg: str) -> list[int]:
    """
        '5' -> 5
        'all' -> All the round that finished in that season
        'latest' -> last round of that season
    """
    if round_arg.isdigit():
        return [int(round_arg)]

    import fastf1
    import pandas as pd

    schedule = fastf1.get_event_schedule(season, include_testing = False)
    today = pd.Timestamp(datetime.now(timezone.utc).date())
    finished = schedule[pd.to_datetime(schedule["EventDate"]) < today]
    rounds = finished["RoundNumber"].astype(int).tolist()

    if round_arg.lower() == "all":
        return rounds
    elif round_arg.lower() == "latest":
        return rounds[-1:]
    logger.error(f"--round must be a number, 'all' or 'latest' (got '{round_arg}')")
    raise ValueError

def _season(value :str) -> int:
    return datetime.now(timezone.utc).year if value == "current" else int(value)

#STAGEs

def run_extract(args):
    from extract.api_client import JolicaClient
    from extract.fastf1_extractor import FastF1Extractor
    from extract.scraper import StatsF1
    from streaming.producer import KafkaProducer
    from streaming.topic_manager import KafkaTopicManager

    sources = {s.strip().lower() for s in args.source.split(",")}
    unknow_sources = sources - {"jolpica", "statsf1", "fastf1"}
    if unknow_sources:
        logger.error(f"Unknown source(s): {unknow_sources}")
        raise ValueError

    with KafkaTopicManager(KAFKA) as topic_manager:
        topic_manager.init_topics()

    producer = KafkaProducer(bootstrap_servers=KAFKA)
    jolpica = JolicaClient() if "jolpica" in sources else None
    statsf1 = StatsF1() if "statsf1" in sources else None
    fastf1 = FastF1Extractor() if "fastf1" in sources else None

    def publish(result :dict, mapping :dict):
        for key, topic in mapping.items():
            producer.send_many(topic_name = topic,
                               messages = as_records(result.get(key)))
        producer.flush()

    failures = []
    try:
        for season in range(_season(args.season), _season(args.to_season or args.season) + 1):
            rounds = resolve_rounds(season, args.round)
            logger.info(f"Season {season}: rounds to extract = {rounds}")
            for rnd in rounds:
                jobs = []
                if jolpica:
                    jobs.append(("jolpica", lambda: publish(jolpica.run(season = season, round = rnd), JOLPICA_TOPICS)))
                if statsf1:
                    jobs.append(("statsf1", lambda : publish(statsf1.run(season = season, round = rnd), STATSF1_TOPICS)))
                if fastf1:
                    for ss in args.sessions.split(","):
                        jobs.append((f"fastf1-{ss}", lambda ss=ss: publish(fastf1.run(season = season, round = rnd, session_type = ss), FASTF1_TOPICS)))

                for name, job in jobs:
                    try:
                        job()
                    except Exception:
                        logger.exception(f"Extract failed: {name} season={season} round={rnd}") 
                        failures.append(f"{name}:{season}:{rnd}")

                time.sleep(args.sleep)  # be polite to Jolpica and statsf1 rate limits
    finally:
        producer.close()

    if failures:
        msg = f"{len(failures)} extract job failed at : {failures}"
        if args.allow_partial:
            logger.warning(msg + " (continuing because --allow-partial)")
        else:
            raise RuntimeError(msg)

def run_bronze(spark):
    from transform.bronze.bronze_job import BronzeJob

    bronze = BronzeJob(sparkSession = spark, bootstrapServer = KAFKA)
    for topic in BRONZE_ORDER:
        logger.info(f"[BRONZE] {topic.value}")
        bronze.write_to_bronze(topic).awaitTermination() # Have "avaiableNow" so it gonna end by it self

def run_silver(spark, only=None):
    from transform.silver.silver_job import silverJob
    from transform.silver.silver_table_config import BASE_TABLE_CONFIGS, DERIVED_TABLE_CONFIGS

    job = silverJob(sparkSession = spark)
    for name in SILVER_ORDER:
        if only and name not in only:
            continue
        logger.info(f"[SILVER] {name}")
        if name in BASE_TABLE_CONFIGS:
            query = job.process_base_table(BASE_TABLE_CONFIGS[name])
        elif name in DERIVED_TABLE_CONFIGS:
            query = job.process_derived_table(DERIVED_TABLE_CONFIGS[name])
        else:
            raise KeyError(f"'{name}' is in SILVER_ORDER but not in any silver config")
        if query is not None: # streaming tables return a StreamingQuery, batch ones return None
            query.awaitTermination()

def run_gold(spark, only=None):
    from transform.gold.gold_job import goldJob
    from transform.gold.gold_table_config import GOLD_TABLE_CONFIGS

    job = goldJob(sparkSession = spark)
    for name in GOLD_ORDER:
        if only and name not in only:
            continue
        logger.info(f"[GOLD] {name}")
        job.process_gold_table(GOLD_TABLE_CONFIGS[name])

def run_load(spark, only=None):
    from load.supabase import Supabase

    db = Supabase(sparkSession = spark)
    if only:
        for name in only:
            db.load_to_supabase(name)
    else:
        db.load_all_to_supabase(varify=True)

#CLI

def _tables(args):
    return {t.strip().upper() for t in args.tables.split(",")} if getattr(args, "tables", None) else None

def build_parser():
    p = argparse.ArgumentParser(description="F1 medallion pipeline")
    sub = p.add_subparsers(dest="stage", required=True)
 
    def add_extract_args(sp):
        sp.add_argument("--season", default="current", help="year or 'current'")
        sp.add_argument("--to-season", default=None, help="end year for a range (default: same as --season)")
        sp.add_argument("--round", default="latest", help="number | latest | all")
        sp.add_argument("--source", default="jolpica,statsf1,fastf1")
        sp.add_argument("--sessions", default="R", help="FastF1 session types, comma separated (R,S,Q...)")
        sp.add_argument("--allow-partial", action="store_true", help="don't fail the task if some rounds fail")
        sp.add_argument("--sleep", type=float, default=1.0, help="seconds to wait between rounds")
 
    add_extract_args(sub.add_parser("extract"))
    sub.add_parser("bronze")
    for name in ("silver", "gold", "load"):
        sp = sub.add_parser(name)
        sp.add_argument("--tables", default=None, help="comma separated subset")
    add_extract_args(sub.add_parser("all"))
    return p

def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.stage == "extract":
            run_extract(args)
        elif args.stage == "all":
            run_extract(args)
            with spark_session() as spark:
                run_bronze(spark)
                run_silver(spark)
                run_gold(spark)
                run_load(spark)
        else:
            with spark_session() as spark:
                if args.stage == "bronze":
                    run_bronze(spark)
                elif args.stage == "silver":
                    run_silver(spark, _tables(args))
                elif args.stage == "gold":
                    run_gold(spark, _tables(args))
                elif args.stage == "load":
                    run_load(spark, _tables(args))
    except Exception:
        logger.exception(f"Stage '{args.stage}' failed")
        return 1
    logger.info(f"Stage '{args.stage}' finished OK")
    return 0
 
 
if __name__ == "__main__":
    sys.exit(main())
 










    

