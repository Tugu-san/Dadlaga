from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
    'retries': 5
}

@dag(
    dag_id='etl_example_with_workflow',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['ETL', 'example']
)
def etl_example_workflow():
    @task
    def extract_orders():
        import pandas as pd
        df = pd.read_csv('/opt/airflow/dags/data/online_retail.csv')
        return df

    @task
    def transform_orders(df):
        df['Total'] = df['Quantity'] * df['Price']
        return df

    @task
    def load_to_postgres(df):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="source_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS Pandas_sales_summary (
                Invoice varchar(20),
                StockCode varchar(20),
                Description varchar(255),
                Quantity integer,
                Price float,
                InvoiceDate DATE,
                CustomerID integer,
                Country varchar(255),
                Total NUMERIC
            );
        """)
        for _, row in df.iterrows():
            cur.execute(
                "INSERT INTO Pandas_sales_summary (Invoice, StockCode, Description, Quantity, Price, InvoiceDate, CustomerID, Country, Total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (row["Invoice"], row["StockCode"], row["Description"], int(row["Quantity"]), float(row["Price"]), row["InvoiceDate"], row["CustomerID"], row["Country"], float(row["Total"]))
            )
        conn.commit()
        cur.close()
        conn.close()
    @task
    def transfer_pg_to_pg():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        src = PostgresHook(postgres_conn_id="source_postgres").get_conn()
        dest = PostgresHook(postgres_conn_id="source_dest").get_conn()

        src_cur = src.cursor()
        dest_cur = dest.cursor()

        dest_cur.execute("""
            CREATE TABLE IF NOT EXISTS Pandas_sales_summary (
                Invoice varchar(20),
                StockCode varchar(20),
                Description varchar(255),
                Quantity integer,
                Price float,
                InvoiceDate DATE,
                CustomerID integer,
                Country varchar(255),
                Total NUMERIC
            );
        """)

        src_cur.execute("SELECT * FROM Pandas_sales_summary")
        rows = src_cur.fetchall()
        for row in rows:
            dest_cur.execute(
                "INSERT INTO Pandas_sales_summary (Invoice, StockCode, Description, Quantity, Price, InvoiceDate, CustomerID, Country, Total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row
            )

        dest.commit()
        src_cur.close()
        dest_cur.close()
        src.close()
        dest.close()