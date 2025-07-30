from airflow import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import csv

# ─── Settings ────────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='sftp_to_postgres_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 7, 28),
    schedule=None,
    catchup=False
)

# ─── Step 0: Create PostgreSQL Table ─────────────────────────────────────────
def create_table():
    pg_hook = PostgresHook(postgres_conn_id='dest_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS my_table (
            Order_ID int primary key,
            Customer_ID int,
            Customer_Name varchar(50),
            Product TEXT,
            Quantity int,
            Price float,
            Order_Date date,
            Region TEXT
        );
    """)
    conn.commit()

create_table_task = PythonOperator(
    task_id='create_postgres_table',
    python_callable=create_table,
    dag=dag
)

# ─── Step 1: Download CSV from SFTP ──────────────────────────────────────────
download_csv = SFTPOperator(
    task_id='download_csv_from_sftp',
    ssh_conn_id='sftp_conn',
    local_filepath='/opt/airflow/include/orders.csv',
    remote_filepath='/upload/data.csv',
    operation='get',
    create_intermediate_dirs=True,
    dag=dag
)

# ─── Step 2: Load CSV into Postgres ──────────────────────────────────────────
def load_csv_to_postgres():
    pg_hook = PostgresHook(postgres_conn_id='dest_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    file_path = '/opt/airflow/include/orders.csv'

    with open(file_path, 'r', encoding='utf-8') as f:
        next(f)  # Skip header
        cursor.copy_expert(
            sql="""
            COPY my_table(Order_ID, Customer_ID, Customer_Name, Product, Quantity, Price, Order_Date, Region)
            FROM STDIN WITH CSV
            """,
            file=f
        )
    conn.commit()

load_to_postgres = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    dag=dag
)

# ─── DAG Dependencies ────────────────────────────────────────────────────────
create_table_task >> download_csv >> load_to_postgres