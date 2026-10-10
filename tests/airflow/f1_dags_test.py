import importlib.util
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path

import pytest


class FakeDAG:
    active_dag = None
    created_dags = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.dag_id = kwargs["dag_id"]
        self.tasks = {}
        self.created_dags.append(self)

    def __enter__(self):
        type(self).active_dag = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        type(self).active_dag = None
        return False


class FakeBashOperator:
    def __init__(self, task_id, bash_command, **kwargs):
        self.task_id = task_id
        self.bash_command = bash_command
        self.kwargs = kwargs
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        FakeDAG.active_dag.tasks[task_id] = self

    def __rshift__(self, other):
        targets = other if isinstance(other, list) else [other]
        for target in targets:
            self.downstream_task_ids.add(target.task_id)
            target.upstream_task_ids.add(self.task_id)
        return other

    def __rrshift__(self, other):
        sources = other if isinstance(other, list) else [other]
        for source in sources:
            source.downstream_task_ids.add(self.task_id)
            self.upstream_task_ids.add(source.task_id)
        return self


@pytest.fixture
def dags_module(monkeypatch):
    FakeDAG.created_dags = []
    FakeDAG.active_dag = None

    airflow_modules = {
        "airflow": types.ModuleType("airflow"),
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.standard": types.ModuleType("airflow.providers.standard"),
        "airflow.providers.standard.operators": types.ModuleType(
            "airflow.providers.standard.operators"
        ),
        "airflow.providers.standard.operators.bash": types.ModuleType(
            "airflow.providers.standard.operators.bash"
        ),
        "airflow.sdk": types.ModuleType("airflow.sdk"),
    }
    for module_name, module in airflow_modules.items():
        module.__path__ = []
        monkeypatch.setitem(sys.modules, module_name, module)

    airflow_modules["airflow.providers.standard.operators.bash"].BashOperator = (
        FakeBashOperator
    )
    airflow_modules["airflow.sdk"].DAG = FakeDAG

    dag_path = (
        Path(__file__).resolve().parents[2] / "airflow" / "dags" / "f1_dags.py"
    )
    spec = importlib.util.spec_from_file_location("f1_dags_under_test", dag_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {dag.dag_id: dag for dag in FakeDAG.created_dags}


@pytest.mark.parametrize(
    ("dag_id", "schedule", "params", "extract_args"),
    [
        (
            "f1_incremental",
            "0 6 * * 1",
            {"season": "current", "round": "latest"},
            "--season {{ params.season }} --round {{ params.round }}",
        ),
        (
            "f1_backfill",
            None,
            {"from_season": 2023, "to_season": 2023},
            "--season {{ params.from_season }} --to-season {{ params.to_season }} "
            "--round all --allow-partial",
        ),
    ],
)
def test_dag_configuration_and_tasks(
    dags_module, dag_id, schedule, params, extract_args
):
    dag = dags_module[dag_id]
    assert dag.kwargs == {
        "dag_id": dag_id,
        "description": (
            "Load the latest finished race through Bronze -> Silver -> Gold -> Supabase"
            if dag_id == "f1_incremental"
            else "Manual: load a range of seasons (start with 1-2 seasons, telemetry is large)"
        ),
        "schedule": schedule,
        "start_date": datetime(2026, 1, 1),
        "catchup": False,
        "max_active_runs": 1,
        "default_args": {"retries": 1, "retry_delay": timedelta(minutes=5)},
        "params": params,
        "tags": ["f1"],
    }

    assert set(dag.tasks) == {
        "extract_jolpica",
        "extract_statsf1",
        "extract_fastf1",
        "bronze",
        "silver",
        "gold",
        "load_supabase",
    }
    for source in ("jolpica", "statsf1", "fastf1"):
        task = dag.tasks[f"extract_{source}"]
        assert task.bash_command == (
            "cd /opt/airflow/src && python pipeline.py extract "
            f"--source {source} {extract_args}"
        )
        assert task.kwargs == {}
        assert task.upstream_task_ids == set()
        assert task.downstream_task_ids == {"bronze"}

    expected_commands = {
        "bronze": "cd /opt/airflow/src && python pipeline.py bronze",
        "silver": "cd /opt/airflow/src && python pipeline.py silver",
        "gold": "cd /opt/airflow/src && python pipeline.py gold",
        "load_supabase": "cd /opt/airflow/src && python pipeline.py load",
    }
    expected_upstream = {
        "bronze": {"extract_jolpica", "extract_statsf1", "extract_fastf1"},
        "silver": {"bronze"},
        "gold": {"silver"},
        "load_supabase": {"gold"},
    }
    expected_downstream = {
        "bronze": {"silver"},
        "silver": {"gold"},
        "gold": {"load_supabase"},
        "load_supabase": set(),
    }
    for task_id, command in expected_commands.items():
        task = dag.tasks[task_id]
        assert task.bash_command == command
        assert task.kwargs == {"execution_timeout": timedelta(hours=3)}
        assert task.upstream_task_ids == expected_upstream[task_id]
        assert task.downstream_task_ids == expected_downstream[task_id]