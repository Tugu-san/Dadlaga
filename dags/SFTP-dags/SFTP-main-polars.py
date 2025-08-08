from __future__ import annotations

import pendulum
import polars as pl
import io

from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Connection IDs ---
SFTP_CONN_ID = "sftp_conn"
POSTGRES_CONN_ID = "source_postgres"

@dag(
    dag_id="sftp_to_postgres_polars_v2",
    start_date=pendulum.datetime(2025, 8, 5, tz="UTC"),
    schedule="@daily",
    catchup=False,
)
def sftp_to_postgres_dag():

    @task
    def download_and_load_to_postgres(**kwargs):
        execution_date = kwargs["ds"]
        date_str = pendulum.parse(execution_date).strftime("%Y%m%d")
        file_name = f"Online_Retail-{date_str}.csv"
        remote_path = f"/upload/{file_name}"

        print(f"Attempting to download file: {remote_path}")

        sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        sftp_client = None
        conn = None
        cursor = None

        try:
            sftp_client = sftp_hook.get_conn()
            print("Successfully connected to SFTP server.")

            with sftp_client.open(remote_path, "rb") as f:
                file_content = f.read()

            print(f"Successfully downloaded file '{file_name}' content into memory.")
            schema_overrides = {"Invoice": pl.Utf8}
            df = pl.read_csv(io.BytesIO(file_content), encoding="latin1", schema_overrides=schema_overrides, has_header=True)
            print("Successfully read CSV data into Polars DataFrame.")
            print(f"DataFrame contains {df.shape[0]} rows.")

            df = df.with_columns([
                pl.col("CustomerID").cast(pl.Int64, strict=False),
                pl.col("InvoiceDate").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            ])
            conn = postgres_hook.get_conn()
            cursor = conn.cursor()
            print("Successfully connected to PostgreSQL.")

            truncate_sql = "TRUNCATE TABLE online_retail;"
            cursor.execute(truncate_sql)
            print("Table 'online_retail' truncated to prepare for new data.")

            buffer = io.StringIO()
            df.write_csv(buffer, separator="\t", include_header=False)
            buffer.seek(0)

            copy_sql = """
            COPY online_retail(Invoice, StockCode, Description, Quantity, InvoiceDate, Price, CustomerID, Country) 
            FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '');
            """

            cursor.copy_expert(sql=copy_sql, file=buffer)
            conn.commit()

            print(f"Successfully inserted {df.shape[0]} records into 'online_retail' table.")

        except FileNotFoundError:
            print(f"File '{file_name}' not found on SFTP server at path '{remote_path}'. Skipping task.")
        except Exception as e:
            print(f"An error occurred: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
            raise
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()
            print("Database connection closed.")

    download_and_load_to_postgres()

sftp_to_postgres_dag()
