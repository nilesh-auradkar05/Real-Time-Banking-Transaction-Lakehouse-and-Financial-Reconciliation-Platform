"""DAG import check. Runs only where Airflow is installed (the CI `dags` job)."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "docker" / "dags"


def _upstream(dag):
    return {t.task_id: set(t.upstream_task_ids) for t in dag.tasks}


def test_dags_import_cleanly():
    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    dag = bag.dags.get("banking_s3_snowflake")
    assert dag is not None
    assert _upstream(dag) == {
        "check_cdc_position": set(),
        "load_raw": {"check_cdc_position"},
        "load_audit_feeds": {"load_raw"},
        "sync_replay_audit": set(),
        "build_models": {"load_audit_feeds", "sync_replay_audit"},
        "optional_reconciliation": {"build_models"},
    }
    assert dag.max_active_runs == 1 and not dag.catchup


def test_preflight_dag_is_manual_and_ordered():
    bag = DagBag(dag_folder=str(DAGS))
    dag = bag.dags.get("banking_preflight")
    assert dag is not None and dag.schedule is None
    assert _upstream(dag) == {
        "check_mounts": set(),
        "check_dbt_connection": {"check_mounts"},
        "check_snowflake_objects": {"check_dbt_connection"},
        "check_source_db": {"check_mounts"},
    }