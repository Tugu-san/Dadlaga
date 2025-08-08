from datetime import datetime, timedelta
import os

from airflow.models.dag import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor

SFTP_CONN_ID = "sftp_conn"
POSTGRES_CONN_ID = "source_postgres"

REMOTE_FILE_PATH = "/upload/Online_Retail-20250807.csv"
LOCAL_FILE_PATH = '/opt/airflow/include/turshilt/Online_Retail-20250807.csv'
POSTGRES_TABLE_NAME = "online_retail"


def load_csv_to_postgres_client_side(local_file_path: str, table_name: str, postgres_conn_id: str):
    print(f"Starting client-side COPY for {local_file_path} to table {table_name}.")
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    # The copy_expert method in the hook is designed for this exact scenario.
    # It performs a client-side copy, streaming the data to the server.
    # The SQL command uses `STDIN` to indicate that data will be streamed.
    postgres_hook.copy_expert(
        sql=f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')",
        filename=local_file_path,
    )
    print("Client-side COPY complete.")

with DAG(
    dag_id='SFTP-to-postgres-with-sensors',
    start_date=datetime(2025, 8, 6),
    schedule='@daily',
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
) as dag:
    
    wait_for_file = SFTPSensor(
        task_id="wait_for_sftp_file",
        sftp_conn_id=SFTP_CONN_ID,
        path=REMOTE_FILE_PATH,
        poke_interval=60,         # check every 60 seconds
        timeout=60 * 60 * 2,       # fail after 2 hours
        mode='poke',               # poke mode is fine for short-lived DAGs
    )

    download_file = SFTPOperator(
        task_id="download_file_from_sftp",
        ssh_conn_id=SFTP_CONN_ID,
        remote_filepath=REMOTE_FILE_PATH,
        local_filepath=LOCAL_FILE_PATH,
        operation="get",  # 'get' means download, 'put' means upload.
    )

    load_data_to_postgres = PythonOperator(
        task_id='sftp_to_postgres_transfer',
        python_callable=load_csv_to_postgres_client_side,
        op_kwargs={
            "local_file_path": LOCAL_FILE_PATH,
            "table_name": POSTGRES_TABLE_NAME,
            "postgres_conn_id": POSTGRES_CONN_ID,
        },
    )

    wait_for_file >> download_file >> load_data_to_postgres