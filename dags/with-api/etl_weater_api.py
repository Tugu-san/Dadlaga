from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import psycopg2

API_KEY = '3a9a94c3bed145b3a6515708250907'
CITY = 'Ulaanbaatar'
API_URL = f'http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}'

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
     dag_id='weather_etl_pipeline',
     default_args=default_args,
     start_date=datetime(2025, 1, 1),
     schedule='@hourly',
     catchup=False
    )
def weather_etl():

    @task()
    def extract():
        response = requests.get(API_URL)
        return response.json()

    @task()
    def transform(data: dict):
        df = pd.DataFrame([{
            'city': data['location']['name'],
            'country': data['location']['country'],
            'temperature_c': data['current']['temp_c'],
            'condition': data['current']['condition']['text'],
            'humidity': data['current']['humidity'],
            'maxwind_kph': data['current']['wind_kph'],
            'last_updated': data['current']['last_updated']
        }])
        return df.to_dict(orient='records')

    @task()
    def load(data: list):
        pg_hook = PostgresHook(postgres_conn_id="source_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                city TEXT,
                country TEXT,
                temperature_c REAL,
                condition TEXT,
                last_updated TIMESTAMP
            )
        """)
        for row in data:
            cur.execute("""
                INSERT INTO weather (city, country, temperature_c, condition, last_updated)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['city'], row['country'], row['temperature_c'], row['condition'], row['last_updated'])
            )
        conn.commit()
        cur.close()
        conn.close()

    weather_data = extract()
    cleaned_data = transform(weather_data)
    load(cleaned_data)

dag = weather_etl()