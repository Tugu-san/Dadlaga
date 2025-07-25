import requests
from bs4 import BeautifulSoup
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id="1212_data_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["1212", "Data Extraction"]
)
def dag_with_212_data():
    
    @task()
    def extract_data():
        url = "https://www2.1212.mn/tablesdata1212.aspx?ln=Mn&tbl_id=DT_NSO_0300_002V1&SOUM_select_all=1&SOUMSingleSelect=&YearY_select_all=1&YearYSingleSelect=&viewtype=table"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        table = soup.find('table')
        df = pd.read_html(str(table))[0]
        return df
    
    @task()
    def transform_data(df):
        df_transposed = df.transpose()
        df_transposed = df_transposed.reset_index()
        df_transposed = df_transposed.drop(1, axis=0).reset_index(drop=True)
        df_transposed = df_transposed.drop(27, axis=1).reset_index(drop=True)
        df_transposed.columns = df_transposed.iloc[0].reset_index(drop=True)
        df_transposed = df_transposed[1:].reset_index(drop=True)
        return df_transposed

    @task()
    def save_to_postgres(df_transposed):
        pg_hook = PostgresHook(postgres_conn_id="dest_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Dynamically create table based on dataframe columns
        columns = ', '.join([f'"{col}" VARCHAR(255)' for col in df_transposed.columns])
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS data_Hun_amiin (
            {columns}
            );
        """)

        for _, row in df_transposed.iterrows():
            columns = ', '.join([f'"{col}"' for col in df_transposed.columns])
            placeholders = ', '.join(['%s'] * len(df_transposed.columns))
            insert_query = f'INSERT INTO data_Hun_amiin ({columns}) VALUES ({placeholders})'
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
    
    df = extract_data()
    df_transformed = transform_data(df)
    save_to_postgres(df_transformed)

dag_with_212_data_instance = dag_with_212_data()