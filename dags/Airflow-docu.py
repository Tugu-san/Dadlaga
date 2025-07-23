from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def my_function(start_date):
    print(f"Task started on: {start_date}")
    return "Task completed successfully"

with DAG(
    dag_id='Context_practice_DAG',
    start_date=datetime(2024, 8, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    task1 = PythonOperator(
        task_id='print_start_date',
        python_callable=my_function,
        op_kwargs={'start_date': '{{ ds }}'},
        dag=dag
    )

    task2 = BashOperator(
        task_id='print_context',
        bash_command='echo "DAG ID: {{ dag.dag_id }}, Task ID: {{ task.task_id }}, Execution Date: {{ ds }}"',
        dag=dag
    )

    task1 >> task2
