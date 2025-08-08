from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime, timedelta

import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

SFTP_CONN_ID = 'sftp_conn'
LOCAL_DIR = '/opt/airflow/include/turshilt'
REMOTE_DIR = 'upload/'
FILE_LIST_PATH = '/opt/airflow/include/files_to_upload.txt'


with DAG(
    dag_id='upload_csv_to_sftp',
    default_args=default_args,
    description='Uploads a local CSV file to the SFTP server',
    start_date=datetime(2025, 7, 28),
    schedule='* 23 * * *',
    catchup=False,
    tags=['sftp', 'csv', 'upload'],
) as dag:
    
    def upload_files_from_list():
        sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        sftp_client = sftp_hook.get_conn()

        with open(FILE_LIST_PATH, 'r') as f:
            file_names = [line.strip() for line in f if line.strip()]

        for file_name in file_names:
            local_path = os.path.join(LOCAL_DIR, file_name)
            remote_path = f"{REMOTE_DIR}/{file_name}"

            if not os.path.exists(local_path):
                raise FileNotFoundError(f"File not found: {local_path}")

            # Upload
            sftp_client.put(local_path, remote_path)
            print(f"Uploaded: {file_name}")

    upload_task = PythonOperator(
        task_id='upload_files_from_txt_list',
        python_callable=upload_files_from_list
    )

upload_task