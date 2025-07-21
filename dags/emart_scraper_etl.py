from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# 🔁 Parent ID Mapping
PARENT_MAP = {
    'Жимс, Хүнсний ногоо': 1,
    'Өдөр тутмын шинэ хүнс': 2,
    'Амттан': 3,
    'Боловсруулсан хүнс': 4,
    'Шингэн хүнс': 5,
    'Гоо сайхан': 6,
    'Хүүхдийн бүтээгдэхүүн': 7,
    'Аялал Спортын бараа': 8,
    'Бичиг, соёлын бараа': 9,
    'Цахилгаан бараа': 10,
    'Багаж хэрэгсэл': 11,
    'Гэр Ахуй': 1,
    'Бэлэг дурсгал, чимэглэл': 13,
    'Тоглоом': 14,
    'Тэжээвэр амьтны хэрэгсэл': 341,
    'Мах махан бүтээгдэхүүн': 740,
    'Даршилсан, нөөшилсөн хүнс': 29,
    'Хөлдөөсөн бүтээгдэхүүн': 696,
    'Starlink': 782,
    'Эрүүл мэндийн хүнс': 344,
    'Бэлэн хувцас': 455,
    'Ахуйн бүтээгдэхүүн': 78,
    'Beauty': 745
}

API_URL = "https://restapi.emartmall.mn:10443/mn/api/category/menu"

@dag(
    dag_id="category_api_to_postgres",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["emart", "category"]
)
def category_etl():

    @task()
    def extract_categories():
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()["data"]
        
        # Зөвхөн шаардлагатай хэсгийг авах
        result = []
        for item in data:
            parent_id = item.get("parentid")
            
            result.append({
                "id": item["id"],
                "name": item["name"],
                "route": item["route"],
                "category": parent_id
            })
        return result

    @task()
    def load_to_postgres(categories):
        pg_hook = PostgresHook(postgres_conn_id="source_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Emart_product (
                id integer primary key,
                Name varchar(225),
                cateroty_id varchar(255),
                Parent_category integer
            );             
        """)
        insert_sql = """
            INSERT INTO Emart_product (id, name, cateroty_id , Parent_category)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """
        for cat in categories:
            cur.execute(insert_sql, (
                cat["id"],
                cat["name"],
                cat["route"],
                cat["category"]
            ))
        conn.commit()
        cur.close()
        conn.close()

    categories = extract_categories()
    load_to_postgres(categories)

category_etl()