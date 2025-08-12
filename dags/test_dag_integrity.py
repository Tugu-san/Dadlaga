# test_dag_integrity.py
import os
import pytest
from airflow.models import DagBag

DAG_PATH = "/opt/airflow/dags" # adjust if needed

@pytest.mark.parametrize("dag_file", [
    os.path.join(DAG_PATH, f)
    for f in os.listdir(DAG_PATH)
    if f.endswith(".py") and not f.startswith("test_")
])
def test_dag_integrity(dag_file):
    dag_bag = DagBag(dag_folder=dag_file, include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
