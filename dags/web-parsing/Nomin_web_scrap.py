import requests
from bs4 import BeautifulSoup
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id="nomin_web_scrap_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["Nomin", "Web Scraping"]
)

def nomin_web_scrap():

    @task()
    def extract_data():
        url = "https://nomin.mn" 
        response = requests.get(url)
        response.raise_for_status()
        html = response.text

        # 2. Parse with BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # 3. Locate each product card
        cards = soup.select("div.group.flex.h-fit")

        products = []
        for card in cards:

            # 4b. Discount badge
            discount_tag = card.select_one("div.bg-red-500")
            discount = discount_tag.get_text(strip=True) if discount_tag else "0%"

            # 4c. Name and description
            name = card.select_one("h3.product-name").get_text(strip=True)
            desc = card.select_one("p.line-clamp-2.truncate").get_text(strip=True)

            # 4d. Prices
            current_price = card.select_one("p.text-primary").get_text(strip=True)
            original_price_tag = card.select_one("p.font-semibold.text-gray-500")
            original_price = original_price_tag.get_text(strip=True) if original_price_tag else ""
            original_price = original_price or current_price

            products.append({
                "name": name,
                "description": desc,
                "current_price": current_price,
                "original_price": original_price,
                "discount": discount,
            })
        df = pd.DataFrame(products)
        df["current_price"] = df["current_price"].str.replace("₮", "").str.replace(",", "").astype(float)
        df["original_price"] = df["original_price"].str.replace("₮", "").str.replace(",", "").astype(float)
        return df

    @task()
    def load_data(df):
        pg_hook = PostgresHook(postgres_conn_id="source_postgres")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nomin_product (
                id SERIAL PRIMARY KEY,
                name varchar(225),
                description varchar(255),
                current_price float,
                original_price float,
                discount varchar(50)
            );             
        """)
        for index, row in df.iterrows():
            # Remove NUL characters from all string fields
            name = str(row["name"]).replace('\x00', '')
            description = str(row["description"]).replace('\x00', '')
            discount = str(row["discount"]).replace('\x00', '')

            cursor.execute(
                "INSERT INTO nomin_product (name, description, current_price, original_price, discount) VALUES (%s, %s, %s, %s, %s)",
                (name, description, row["current_price"], row["original_price"], discount)
            )

        conn.commit()
        cursor.close()
        conn.close()

    df = extract_data()
    load_data(df)

nomin_web_scrap_dag = nomin_web_scrap()