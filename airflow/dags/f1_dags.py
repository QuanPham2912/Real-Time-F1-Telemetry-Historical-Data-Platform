from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
import pendulum
from metadata.setting import get_settings

settings = get_settings()
local_tz = pendulum.timezone(settings.timezone)

CMD = "cd /opt/airflow/src && python pipeline.py"

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def add_pipeline(extract_cmd_args: str):
    """Build extract(3 sources in parallel) -> bronze -> silver -> gold -> load inside the current DAG."""
    extracts = [
        BashOperator(
            task_id=f"extract_{source}",
            bash_command=f"{CMD} extract --source {source} {extract_cmd_args}",
        )
        for source in ("jolpica", "statsf1", "fastf1")
    ]
    heavy = {"execution_timeout": timedelta(hours=3)}
    bronze = BashOperator(task_id="bronze", bash_command=f"{CMD} bronze", **heavy)
    silver = BashOperator(task_id="silver", bash_command=f"{CMD} silver", **heavy)
    gold = BashOperator(task_id="gold", bash_command=f"{CMD} gold", **heavy)
    load = BashOperator(task_id="load_supabase", bash_command=f"{CMD} load", **heavy)

    extracts >> bronze >> silver >> gold >> load


with DAG(
    dag_id="f1_incremental",
    description="Load the latest finished race through Bronze -> Silver -> Gold -> Supabase",
    schedule="0 6 * * 1",  # Monday 06:00 in TIMEZONE
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "season": "current",
        "round": "latest",
    },  # override with "Trigger DAG w/ config"
    tags=["f1"],
):
    add_pipeline("--season {{ params.season }} --round {{ params.round }}")


with DAG(
    dag_id="f1_backfill",
    description="Manual: load a range of seasons (start with 1-2 seasons, telemetry is large)",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"from_season": 2023, "to_season": 2023},
    tags=["f1"],
):
    add_pipeline(
        "--season {{ params.from_season }} --to-season {{ params.to_season }} --round all --allow-partial"
    )
