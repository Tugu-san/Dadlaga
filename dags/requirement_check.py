from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

def get_matplotlib():
    import matplotlib
    print(f'matplotlib with version: {matplotlib.__version__}')

with DAG(
    dag_id="matplotlib_version_Check",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['matplotlib', 'version_check']
) as dag:
    t1 = PythonOperator(
        task_id='get_matplotlib_version',
        python_callable=get_matplotlib
    )

t1