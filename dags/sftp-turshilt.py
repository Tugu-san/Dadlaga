from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='upload_csv_to_sftp',
    default_args=default_args,
    description='Uploads a local CSV file to the SFTP server',
    start_date=datetime(2025, 7, 28),
    schedule=None,
    catchup=False,
    tags=['sftp', 'csv', 'upload'],
) as dag:
    
    upload_csv = SFTPOperator(
        task_id='upload_csv_file',
        ssh_conn_id='sftp_conn',
        local_filepath='/opt/airflow/include/orders.csv',
        remote_filepath = 'upload/data.csv',
        operation='put',
        create_intermediate_dirs=True
    )

    upload_csv
