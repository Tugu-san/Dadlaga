from __future__ import annotations

import pendulum
import pandas as pd
import io

from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Connection IDs ---
SFTP_CONN_ID = "sftp_conn"
POSTGRES_CONN_ID = "source_postgres"

@dag(
    dag_id="sftp_to_postgres_main",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def sftp_to_postgres_dag():

    @task
    def download_and_load_to_postgres(**kwargs):
        """
        This function connects to SFTP, downloads the daily file,
        and uses the PostgresHook to load it into the database.
        """
        # Get the execution date from the Airflow context
        execution_date = kwargs["ds"]
        date_str = pendulum.parse(execution_date).strftime("%Y%m%d")
        file_name = f"Online_Retail-{date_str}.csv"
        remote_path = f"/upload/{file_name}" # Assuming files are in an 'upload' directory

        print(f"Attempting to download file: {remote_path}")

        sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Initialize connections to None to ensure they can be closed in 'finally'
        sftp_client = None
        conn = None
        cursor = None

        try:
            # 1. Get the underlying SFTP client from the hook
            sftp_client = sftp_hook.get_conn()
            print("Successfully connected to SFTP server.")

            # Download file content into memory using the paramiko client
            # This is a more compatible way than using 'read_file'
            with sftp_client.open(remote_path, "rb") as f:
                # The 'rb' mode reads the file as bytes, which is what pandas expects
                file_content = f.read()

            print(f"Successfully downloaded file '{file_name}' content into memory.")

            # Use pandas to read the CSV data from the in-memory bytes
            # The file is expected to be encoded in 'latin1' based on common datasets
            # of this type. Adjust if your encoding is different (e.g., 'utf-8').
            df = pd.read_csv(io.BytesIO(file_content), encoding='latin1')
            print("Successfully read CSV data into pandas DataFrame.")
            print(f"DataFrame contains {len(df)} rows.")

            # Data Cleaning and Preparation
            # Convert column names to a more SQL-friendly format (lowercase, no spaces)
            df.columns = [
                'Invoice', 'StockCode', 'Description', 'Quantity',
                'InvoiceDate', 'Price', 'CustomerID', 'Country'
            ]
            # Convert InvoiceDate to datetime objects
            df['CustomerID'] = pd.to_numeric(df['CustomerID'], errors='coerce').astype('Int64')

            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

            # 2. Connect to PostgreSQL and load data
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            print("Successfully connected to PostgreSQL.")

            # 4. Truncate table to ensure idempotency for the daily load
            # This prevents duplicate data if the task is re-run for the same day.
            truncate_sql = "TRUNCATE TABLE online_retail;"
            cursor.execute(truncate_sql)
            print("Table 'online_retail' truncated to prepare for new data.")

            # 5. Insert data into PostgreSQL
            # Using psycopg2's copy_expert for efficient bulk insertion
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep='\t')
            buffer.seek(0)
            
            copy_sql = """
            COPY online_retail(Invoice, StockCode, Description, Quantity, InvoiceDate, Price, CustomerID, Country) 
            FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '');
            """

            cursor.copy_expert(sql=copy_sql, file=buffer)
            conn.commit()

            print(f"Successfully inserted {len(df)} records into 'online_retail' table.")

        except FileNotFoundError:
            print(f"File '{file_name}' not found on SFTP server at path '{remote_path}'. Skipping task.")
            # You might want to use raise AirflowSkipException here
            # from airflow.exceptions import AirflowSkipException
            # This would mark the task as 'skipped' in the Airflow UI.
        except Exception as e:
            print(f"An error occurred: {e}")
            # Rollback transaction on error
            if 'conn' in locals() and conn:
                conn.rollback()
            raise
        finally:
            # Clean up connections
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
            print("Database connection closed.")

    download_and_load_to_postgres()

sftp_to_postgres_dag()