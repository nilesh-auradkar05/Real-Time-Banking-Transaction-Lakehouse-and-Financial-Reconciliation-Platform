"""DAG import check. Runs only where Airflow is installed (the CI `dags` job)."""
from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "docker" / "dags"


def test_dags_import_cleanly():
    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    dag = bag.dags.get("banking_s3_snowflake")
    assert dag is not None
    assert [t.task_id for t in dag.topological_sort()] == [
        "load_raw", "load_audit_feeds", "build_models", "optional_reconciliation"]
    assert dag.max_active_runs == 1 and not dag.catchup
