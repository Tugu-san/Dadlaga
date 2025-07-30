from airflow import DAG
from airflow.decorators import task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import tempfile

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1
}

with DAG(
    dag_id='sftp_to_postgres_pipeline',
    schedule=None,
    default_args=default_args,
    catchup=False
) as dag:

    @task()
    def download_from_sftp():
        sftp_hook = SFTPHook(ssh_conn_id='sftp_conn')
        remote_path = 'home/sftpuser/upload/text.csv'

        # Түр файл үүсгэх
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp_file:
            local_path = tmp_file.name
            sftp_hook.retrieve_file(remote_path, local_path)

            # pandas ашиглан унших
            df = pd.read_csv(local_path)

        return df.to_dict(orient='records')
    
    @task()
    def insert_to_postgres(records):
        pg_hook = PostgresHook(postgres_conn_id='source_postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            Order_ID int PRIMARY KEY,
            Customer_ID varchar(255),
            Customer_Name text,
            Product text,
            Quantity int,
            Price float,
            Order_Date date,
            Region varchar(255)
        )
    """)
        
        for row in records:
            cursor.execute("""
                INSERT INTO products (Order_ID, Customer_ID, Customer_Name, Product, Quantity, Price, Order_Date, Region)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (Invoice) DO NOTHING
            """, (row['Order_ID'],
                    row['Customer_ID'],
                    row['Customer_Name'],
                    row['Product'],
                    row['Quantity'],
                    row['Price'],
                    row['Order_Date'],
                    row['Region'] ))
        conn.commit()
        cursor.close()

    data = download_from_sftp()
    insert_to_postgres(data)