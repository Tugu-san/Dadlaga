from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import polars as pl

def extract_orders():
    df = pl.read_csv("/opt/airflow/include/online_retail_II_cleaned.csv")
    df.write_parquet("/opt/airflow/include/online_retail_II.parquet")  # дараагийн алхамд хэрэглэх
    return "/opt/airflow/include/online_retail_II.parquet"

def transform_orders(parquet_path: str):
    df = pl.read_parquet(parquet_path)
    
    # Нийт үнийг тооцоолох
    df = df.with_columns(
        (df["Quantity"] * df["Price"]).alias("Total")
    )

    summary_df = (
        df.group_by([
            "Invoice", "StockCode", "Description", "Quantity", 
            "InvoiceDate", "Price", "CustomerID", "Country"
        ])
        .agg([
            pl.sum("Total").alias("Total")
        ])
    )

    summary_df.write_parquet("/opt/airflow/include/Polars_summary.parquet")
    return "/opt/airflow/include/Polars_summary.parquet"

def load_to_postgres(parquet_path: str):
    df = pl.read_parquet(parquet_path)
    pg_hook = PostgresHook(postgres_conn_id="online_retail_default")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Invoice (
            Invoice varchar(20),
            StockCode varchar(20),
            Description varchar(255),
            Quantity integer,
            Price float,
            InvoiceDate DATE,
            CustomerID integer,
            Country varchar(255),
            Total NUMERIC
        )
    """)
    
    for row in df.iter_rows(named=True):
        cur.execute(
            "INSERT INTO Invoice (Invoice, StockCode, Description, Quantity, Price, InvoiceDate, CustomerID, Country, Total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (row["Invoice"], row["StockCode"], row["Description"], int(row["Quantity"]), float(row["Price"]), row["InvoiceDate"], row["CustomerID"], row["Country"], float(row["Total"]))
        )

    conn.commit()
    cur.close()
    conn.close()

def transfer_to_dest_pg():
    src = PostgresHook(postgres_conn_id="online_retail_default").get_conn()
    dest = PostgresHook(postgres_conn_id="online_retail_dest").get_conn()

    src_cur = src.cursor()
    dest_cur = dest.cursor()

    dest_cur.execute("""
        CREATE TABLE IF NOT EXISTS Invoice (
            Invoice varchar(20),
            StockCode varchar(20),
            Description varchar(255),
            Quantity integer,
            Price float,
            InvoiceDate DATE,
            CustomerID integer,
            Country varchar(255),
            Total NUMERIC
        )
    """)

    src_cur.execute("SELECT * FROM sales_summary")
    for row in src_cur.fetchall():
        dest_cur.execute(
            "INSERT INTO Invoice (Invoice, StockCode, Description, Quantity, Price, InvoiceDate, CustomerID, Country, Total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row
        )

    dest.commit()
    src_cur.close()
    dest_cur.close()
    src.close()
    dest.close()

with DAG(
    dag_id="polars_etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["polars", "ETL"]
) as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract_orders
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform_orders,
        op_args=["{{ ti.xcom_pull(task_ids='extract') }}"]
    )

    t3 = PythonOperator(
        task_id="load_to_pg1",
        python_callable=load_to_postgres,
        op_args=["{{ ti.xcom_pull(task_ids='transform') }}"]
    )

    t4 = PythonOperator(
        task_id="transfer_pg_to_pg",
        python_callable=transfer_to_dest_pg
    )

    t1 >> t2 >> t3 >> t4