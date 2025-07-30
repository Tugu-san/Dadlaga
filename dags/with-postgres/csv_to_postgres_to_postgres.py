from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

def load_csv_to_postgres():
    df = pd.read_csv('/opt/airflow/include/data.csv')
    conn = psycopg2.connect(
        host='postgres_source',
        dbname='source_db',
        user='source_user',
        password='source_pass'
    )
    cur = conn.cursor()
    cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
            Product_name TEXT,
            Product_price NUMERIC,
            department TEXT,
            productMaterial TEXT,
            productAdjective TEXT,
            productDescription TEXT,
            Customer_name TEXT,
            Customer_gender TEXT,
            Customer_mail TEXT,
            Customer_address TEXT,
            Customer_phone TEXT,
            Customer_birthdate DATE,
            Order_date DATE,
            Customer_State TEXT,
            Product_Quantity INTEGER,
            Product_ID INTEGER,
            Customer_ID INTEGER
        );
    """)
    for _, row in df.iterrows():
        cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tuple(row))
    conn.commit()
    cur.close()
    conn.close()

def transfer_postgres_to_postgres():
    src = psycopg2.connect(
        host='postgres_source',
        dbname='source_db',
        user='source_user',
        password='source_pass'
    )
    dest = psycopg2.connect(
        host='postgres_dest',
        dbname='dest_db',
        user='dest_user',
        password='dest_pass'
    )
    src_cur = src.cursor()
    dest_cur = dest.cursor()

    dest_cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
            Product_name TEXT,
            Product_price NUMERIC,
            department TEXT,
            productMaterial TEXT,
            productAdjective TEXT,
            productDescription TEXT,
            Customer_name TEXT,
            Customer_gender TEXT,
            Customer_mail TEXT,
            Customer_address TEXT,
            Customer_phone TEXT,
            Customer_birthdate DATE,
            Order_date DATE,
            Customer_State TEXT,
            Product_Quantity BIGINT,
            Product_ID BIGINT,
            Customer_ID BIGINT
        );
    """)
    src_cur.execute("SELECT * FROM orders")
    rows = src_cur.fetchall()
    for row in rows:
        dest_cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
    dest.commit()
    src_cur.close()
    dest_cur.close()
    src.close()
    dest.close()

with DAG(
    dag_id='csv_to_postgres_to_postgres',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv_to_postgres
    )

    t2 = PythonOperator(
        task_id='transfer_data',
        python_callable=transfer_postgres_to_postgres
    )

    t1 >> t2

