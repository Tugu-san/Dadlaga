import csv
import io
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ssh.hooks.ssh import SSHHook

SFTP_CONN_ID = "sftp_conn"
POSTGRES_CONN_ID = "source_postgres"

REMOTE_FILE_PATH = "/upload/Online_Retail-20250801.csv"
LOCAL_FILE_PATH = '/opt/airflow/include/turshilt/Online_Retail-20250801.csv'

POSTGRES_TABLE_NAME = "online_retail"

def sftp_to_postgres_transfer_with_paramiko():
    print("Starting SFTP to PostgreSQL transfer using Paramiko...")

    # --- Task 1: Connect to SFTP and download the file ---
    print(f"Connecting to SFTP server using connection '{SFTP_CONN_ID}'...")
    ssh_hook = SSHHook(ssh_conn_id=SFTP_CONN_ID)
    # The ssh_hook can be used to get a paramiko client directly.

    ssh_client = ssh_hook.get_conn()
    sftp_client = ssh_client.open_sftp()
    print(f"Successfully connected to SFTP server. Downloading file: {REMOTE_FILE_PATH}")

    # Download the file content into an in-memory text buffer.
    # This avoids saving the file to the local disk of the Airflow worker.
    with sftp_client.open(REMOTE_FILE_PATH, "r") as remote_file:
        file_content = remote_file.read().decode("utf-8")
        print(f"File '{REMOTE_FILE_PATH}' downloaded successfully into memory.")

    sftp_client.close()
    ssh_client.close()

    # --- Task 2: Connect to PostgreSQL and load the data ---
    print(f"Connecting to PostgreSQL using connection '{POSTGRES_CONN_ID}'...")
    # Use the PostgresHook to interact with the PostgreSQL database.
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    print("Successfully connected to PostgreSQL.")

    # --- Task 3: Parse CSV and insert data ---
    print("Parsing CSV data and inserting into PostgreSQL...")
    # Use io.StringIO to treat the string content as a file.
    csv_file = io.StringIO(file_content)
    reader = csv.reader(csv_file)

    # Skip the header row if your CSV has one.
    next(reader, None)

    postgres_hook.copy_expert(
        sql=f"COPY {POSTGRES_TABLE_NAME} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')",
        filename=LOCAL_FILE_PATH,
    )
    print("Client-side COPY complete.")

    print(f"Finished data transfer. Inserted data into '{POSTGRES_TABLE_NAME}'.")

with DAG(
    dag_id="sftp_to_postgres_with_paramiko",
    start_date=datetime(2025, 8, 1),
    schedule=None,  # This DAG is manually triggered.
    catchup=False,
    tags=["sftp", "postgres", "paramiko", "example"],
) as dag:

    transfer_task = PythonOperator(
        task_id="sftp_to_postgres_transfer",
        python_callable=sftp_to_postgres_transfer_with_paramiko,
    )

    transfer_task