from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import csv

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

TARGET_FILENAME = "orders.csv"  # <<== энд хүссэн файлаа заана уу
REMOTE_DIR = "/upload"
LOCAL_DIR = "/opt/airflow/include/turshilt"
TABLE_NAME = "orders" # <<== энд хүссэн table заана уу

def download_target_file_from_sftp():
    sftp_hook = SFTPHook(ftp_conn_id='sftp_conn')
    os.makedirs(LOCAL_DIR, exist_ok=True)

    files = sftp_hook.list_directory(REMOTE_DIR)
    print(f"Number of files: {len(files)}")
    if TARGET_FILENAME not in files:
        raise FileNotFoundError(f"{TARGET_FILENAME} not found in {REMOTE_DIR}")
    
    local_file_path = os.path.join(LOCAL_DIR, TARGET_FILENAME)
    remote_file_path = f"{REMOTE_DIR}/{TARGET_FILENAME}"

    sftp_hook.retrieve_file(remote_file_path, local_file_path)
    print(f"Downloaded file: {TARGET_FILENAME}")

def load_csv_to_postgres():
    local_file_path = os.path.join(LOCAL_DIR, TARGET_FILENAME)
    pg_hook = PostgresHook(postgres_conn_id='dest_postgres')
    
    with open(local_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        header = next(reader)

        rows = [tuple(row) for row in reader]
    
    insert_sql = f"""
        INSERT INTO {TABLE_NAME} ({', '.join(header)})
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    pg_hook.insert_rows(TABLE_NAME, rows, target_fields=header)
    print(f"Inserted {len(rows)} rows into {TABLE_NAME}")

with DAG(
    dag_id='sftp_target_file_to_postgres',
    default_args=default_args,
    description='Download a target file from SFTP and load to Postgres',
    schedule=None,
    start_date=datetime(2025, 7, 30),
    catchup=False
) as dag:

    download_file = PythonOperator(
        task_id='download_target_file_from_sftp',
        python_callable=download_target_file_from_sftp
    )

    insert_data = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    download_file >> insert_data
 