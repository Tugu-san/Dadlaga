from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="fetch_events",
    schedule="@daily", #өдөр болгон 00:00 цагт
    start_date=datetime(2025, 1, 1) #2025 оны 1 сарын 1-с
)as dag:
    
    task = BashOperator(
        task_id = "fetch_events",
        bash_command = 'echo "Fetching events from {{ data_interval_start }} to {{ data_interval_end }}"'
    )
    task
