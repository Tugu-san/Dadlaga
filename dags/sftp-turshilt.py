from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 30),
    'retries': 1
}

dag = DAG(
    dag_id='sftp_to_postgres_turshilt',
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["etl", "sftp", "postgres"]
)

LOCAL_FILE_PATH = '/opt/airflow/include/text.csv'
REMOTE_FILE_PATH = 'home/sftpuser/upload/text.csv'

def download_from_sftp():
    hook = SFTPHook(ssh_conn_id='sftp_conn')  # Airflow connection ID
    hook.retrieve_file(REMOTE_FILE_PATH, LOCAL_FILE_PATH)

def load_to_postgres():
    df = pd.read_csv(LOCAL_FILE_PATH)
    pg_hook = PostgresHook(postgres_conn_id='source_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Таблиц үүсгэх (байхгүй бол)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            Invoice varchar(255) PRIMARY KEY,
            StockCode varchar(255),
            Description text,
            Quantity int,
            price float,
            InvoiceDate date,
            CustomerID varchar(255),
            Country varchar(255),
            Total float
        )
    """)

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO products (Invoice, StockCode, Description, Quantity, price, InvoiceDate, CustomerID, Country, Total) " \
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (Invoice) DO NOTHING",
            (row['Invoice'],
                row['StockCode'],
                row['Description'],
                row['Quantity'],
                row['price'],
                row['InvoiceDate'],
                row['CustomerID'],
                row['Country'],
                row['Total']
            )
        )

    conn.commit()
    cursor.close()

download_task = PythonOperator(
    task_id='download_csv_from_sftp',
    python_callable=download_from_sftp,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

download_task >> load_task
