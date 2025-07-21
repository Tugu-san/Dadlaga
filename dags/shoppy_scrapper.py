from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=2)
}

API_URL = "https://shoppy.es.asia-southeast1.gcp.elastic-cloud.com/shoppy_brand/_search"
USERNAME = "guest"
API_PASS = "ShoppyGuest"

@dag(
    dag_id="Shoppy_mn_products_2",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["emart", "category"]
)
def category_etl():

    @task()
    def extract_categories():
        response = requests.get(
            API_URL,
            auth=(USERNAME, API_PASS)
        )
        response.raise_for_status()
        data = response.json()["data"]
        
        # Зөвхөн шаардлагатай хэсгийг авах
        result = []
        for item in data:     
            result.append({
                "pid": item["pid"],
                "name": item["name"],
                "price": item["price"],
                "total_on_hand": item["total_on_hand"],
            })
        return result

    @task()
    def load_to_postgres(categories):
        pg_hook = PostgresHook(postgres_conn_id="source_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Shoppy_mn_product (
                pid integer primary key,
                name varchar(225),
                price float,
                total_on_hand integer
            );             
        """)
        insert_sql = """
            INSERT INTO Emart_product (pid, name, price, total_on_hand)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """
        for cat in categories:
            cur.execute(insert_sql, (
                cat["pid"],
                cat["name"],
                cat["price"],
                cat["total_on_hand"]
            ))
        conn.commit()
        cur.close()
        conn.close()

    categories = extract_categories()
    load_to_postgres(categories)

category_etl()