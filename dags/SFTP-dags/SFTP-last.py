import os
import pandas as pd
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator

# --- Тогтмолууд (Өөрийн орчинд тохируулна уу) ---
SFTP_CONN_ID = "sftp_conn"
POSTGRES_CONN_ID = "source_postgres"
POSTGRES_TABLE_NAME = "online_retail"
SFTP_PATH = "/upload"  # SFTP сервер дээрх файлын зам
LOCAL_TEMP_PATH = "/opt/airflow/include/turshilt" # Worker дээр түр хадгалах зам

@dag(
    dag_id="sftp_online_retail_to_postgres_upsert_v1",
    start_date=datetime(2025, 8, 8),
    schedule="@daily",
    catchup=True,
    tags=['sftp', 'postgres', 'etl', 'daily-load'],
    doc_md="""
    ### SFTP to PostgreSQL ETL DAG (with Upsert Logic)

    Энэхүү DAG нь SFTP серверээс өдөр бүрийн Online Retail өгөгдлийг татаж, 
    файлын нэрнээс огноог салган нэмэлт багана болгон PostgreSQL хүснэгтэд хадгална.
    Хэрэв тухайн огноотой өгөгдөл аль хэдийн орсон бол **шинэ өгөгдлөөр бүрэн сольж хадгална (upsert)**.
    """
)
def sftp_to_postgres_etl():
    """
    SFTP-с өдөр тутмын Online Retail өгөгдлийг татаж PostgreSQL-д ачаалах DAG.
    """
    # DAG-ийн ажиллаж буй огнооноос хамаарч файлын нэрийг динамикаар үүсгэнэ.
    # Жишээ нь: 2025-08-01 өдрийн DAG-д Online_Retail-20250801.csv файлыг хайна.
    # {{ ds_nodash }} нь YYYYMMDD форматтай огноог илэрхийлдэг Airflow-н хувьсагч юм.
    file_name_template = f"Online_Retail-{{{{ ds_nodash }}}}.csv"
    
    sftp_file_path_template = os.path.join(SFTP_PATH, file_name_template)

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Task 1: SFTP сервер дээр файл орж ирсэн эсэхийг шалгах Sensor
    wait_for_file = SFTPSensor(
        task_id="wait_for_sftp_file",
        sftp_conn_id=SFTP_CONN_ID,
        path=sftp_file_path_template,
        poke_interval=60,  # 60 секунд тутамд шалгана
        timeout=60 * 30,   # 30 минут хүлээгээд алдаа заана
        mode="poke"
    )

    @task
    def download_file_from_sftp(**kwargs):
        """SFTP-с файлыг татаж, локал түр замд хадгална."""
        # Файлын болон замын нэрийг Airflow-н context-оос авна
        file_name = f"Online_Retail-{kwargs['ds_nodash']}.csv"
        remote_file_path = os.path.join(SFTP_PATH, file_name)
        local_file_path = os.path.join(LOCAL_TEMP_PATH, file_name)
        
        os.makedirs(LOCAL_TEMP_PATH, exist_ok=True) # Түр хавтас байхгүй бол үүсгэнэ
        sftp_hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)

        print(f"Downloading {remote_file_path} to {local_file_path}...")
        sftp_hook.retrieve_file(remote_file_path, local_file_path)
        print("Download complete.")
        return local_file_path

    @task
    def process_and_load_to_postgres(local_fpath: str, **kwargs):
        """
        Файлыг боловсруулж, огноог нэмээд, PostgreSQL-д upsert хийнэ.
        """
        execution_date = kwargs["ds"] # YYYY-MM-DD форматтай огноо
        print(f"Processing data for date: {execution_date}")
        
        # CSV файлыг pandas DataFrame болгон унших
        df = pd.read_csv(local_fpath)

        # Шаардлагатай 'uploaded_date' баганыг нэмэх
        df['uploaded_date'] = pd.to_datetime(execution_date).date()

        # Postgres-д холбогдох hook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        print(f"Starting transaction to upsert data for {execution_date} into {POSTGRES_TABLE_NAME}.")
        
        try:
            # Алхам 1: Хуучин өгөгдлийг устгах (DELETE)
            # Энэ нь idempotency буюу үйлдлийг хэдэн ч удаа давтсан үр дүн ижил байх нөхцөлийг хангана.
            delete_sql = f"DELETE FROM {POSTGRES_TABLE_NAME} WHERE uploaded_date = %s"
            print(f"Executing: {delete_sql} with date {execution_date}")
            cursor.execute(delete_sql, (execution_date,))
            deleted_rows = cursor.rowcount
            print(f"{deleted_rows} rows deleted for date {execution_date}.")

            # Алхам 2: Шинэ өгөгдлийг оруулах (INSERT)
            # copy_expert нь маш их хэмжээний өгөгдлийг хурдан оруулах хамгийн үр дүнтэй арга.
            buffer = StringIO()
            # Таны хүснэгтийн баганын дарааллаар CSV-г бэлтгэнэ
            df_columns_ordered = ['Invoice', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'Price', 'CustomerID', 'Country', 'uploaded_date']
            df[df_columns_ordered].to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            copy_sql = f"COPY {POSTGRES_TABLE_NAME} ({','.join(df_columns_ordered)}) FROM STDIN WITH (FORMAT CSV)"
            
            print(f"Executing COPY command to insert {len(df)} new rows...")
            cursor.copy_expert(sql=copy_sql, file=buffer)
            
            # Трансакцыг баталгаажуулах
            conn.commit()
            print("Transaction successful. Data has been upserted.")

        except Exception as e:
            print(f"Error during transaction: {e}")
            # Алдаа гарвал хийсэн бүх өөрчлөлтийг буцаах
            conn.rollback()
            raise
        finally:
            # Холболтыг үргэлж хаах
            cursor.close()
            conn.close()

    @task
    def cleanup_local_file(local_fpath: str):
        """Worker-ийн дискнээс татаж авсан файлыг устгана."""
        if os.path.exists(local_fpath):
            print(f"Cleaning up local file: {local_fpath}")
            os.remove(local_fpath)
        else:
            print(f"File {local_fpath} not found for cleanup, skipping.")

    # Task-уудын хамаарлыг тодорхойлох (TaskFlow API)
    downloaded_path = download_file_from_sftp()
    processed_data_task = process_and_load_to_postgres(downloaded_path)
    
    start >> wait_for_file >> downloaded_path
    processed_data_task >> cleanup_local_file(downloaded_path) >> end


# DAG-г үүсгэх
sftp_to_postgres_etl()