from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import psycopg2

API_KEY = '3a9a94c3bed145b3a6515708250907'


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='multi-cities-weather_etl',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='@hourly',
    catchup=False
)
def wheater_multi_etl():
    cities = ['Ulaanbaatar', 'Tokyo', 'New Yors', 'Seoul']

    @task()
    def extract(city):
        url = f'http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={city}'
        response = requests.get(url)
        data = response.json()
        return{
            'city': city,
            'data': data
        }

    @task()
    def transform_data(city_data):
        data = city_data['data']
        city = city_data['city']
        df = pd.DataFrame([{
            'city': city,
            'country': data['location']['country'],
            'temperature_c': data['current']['temp_c'],
            'condition': data['current']['condition']['text'],
            'humidity': data['current']['humidity'],
            'wind_kph': data['current']['wind_kph'],
            'last_updated': data['current']['last_updated']
        }])
        return df.to_dict(orient='records')

    @task()
    def load(rows):
        pg_hook = PostgresHook(postgres_conn_id="source_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS multi_city_weather (
                city TEXT,
                country TEXT,
                temperature_c REAL,
                condition TEXT,
                humidity float,
                wind_kph float,
                last_updated TIMESTAMP
            )
        """)
        for row in rows:
            cur.execute("""
                INSERT INTO multi_city_weather (city, country, temperature_c, condition, humidity, wind_kph, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['city'], row['country'], row['temperature_c'],
                row['condition'], row['humidity'], row['wind_kph'],row['last_updated']
            ))
        conn.commit()
        cur.close()
        conn.close()

    extracted = extract.expand(city=cities)
    transformed = transform_data.expand(city_data=extracted)
    load.expand(rows=transformed)

dag = wheater_multi_etl()

