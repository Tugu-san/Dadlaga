# tests/dags/test_dag_integrity.py
import glob
import os
import pytest
from airflow.dag_processing.processor import DagFileProcessor

# dags хавтасны замыг тодорхойлно
DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/**/*.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)

@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file, caplog):
    """Бүх DAG файлуудын бүрэн бүтэн байдлыг шалгана."""
    
    # Airflow-н DAG унших процессыг дуурайна
    DagFileProcessor._get_dagbag(dag_file)

    # DAG унших үед гарсан алдааны лог байгаа эсэхийг шалгана
    for record in caplog.records:
        if record.levelname == "ERROR":
            # Хэрэв алдаа байвал тестийг fail болгоно
            raise record.exc_info