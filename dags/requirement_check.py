import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.sftp.operators.sftp import SFTPOperator
from datetime import datetime, timedelta

files_dir = '/opt/airflow/sftp-data'  # Local folder inside Airflow container
remote_dir = '/upload/'               # Remote path on SFTP server

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='upload_new_csvs_to_sftp',
    default_args=default_args,
    description='Upload all CSVs in local folder to SFTP',
    schedule='*/5 * * * *',
    start_date=datetime(2025, 7, 28),
    catchup=False,
    tags=['sftp', 'csv', 'monitor'],
) as dag:

    @task
    def list_csv_files():
        return [
            {
                "local_filepath": os.path.join(files_dir, file),
                "remote_filepath": os.path.join(remote_dir, file),
            }
            for file in os.listdir(files_dir)
            if file.endswith('.csv')
        ]

    upload_csv = SFTPOperator.partial(
        task_id='upload_csv_file',
        ssh_conn_id='sftp_conn',
        operation='put',
        create_intermediate_dirs=True,
    ).expand_kwargs(list_csv_files())
