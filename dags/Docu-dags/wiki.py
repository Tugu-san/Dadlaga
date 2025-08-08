# Файлын нэр: dags/wikipedia_pageviews_dag.py
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Энэ функц нь задлагдсан файлаас өгөгдлийг уншиж,
# SQL INSERT query үүсгэж, файлд бичнэ.
def _fetch_pageviews(data_interval_start, pagenames):
    # Үр дүнг хадгалах dict-г анхны утгаар нь 0 гэж үүсгэнэ
    result = dict.fromkeys(pagenames, 0)
    
    # Задлагдсан файлыг унших горимоор нээнэ
    with open(f"/opt/airflow/include/wikipageviews/wikipageviews-{data_interval_start.format('YYYYMMDDHH')}", "r") as f: 

        for line in f:
            # Мөр бүрийг зайгаар нь салгаж, хэрэгтэй мэдээллээ авна
            domain_code, page_title, view_counts, _ = line.split(" ")
            
            # Зөвхөн англи ("en") домэйн дээрх, бидний сонирхсон хуудсуудыг шүүнэ
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts 

    # Шүүсэн үр дүнгээ ашиглан SQL query-үүд үүсгэж, файлд бичнэ
    with open("/opt/airflow/include/wikipageviews/wikipageviews/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts (pagename, pageviewcount, datetime) VALUES ("
                f"'{pagename}', {pageviewcount}, '{data_interval_start}'"
                ");\n"
            )

with DAG(
    dag_id="stock_sense_wikipedia_pageviews",
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="@hourly",
    template_searchpath="/opt/airflow/include/wikipageviews/wikipageviews",  # .sql файлыг хайх замыг зааж өгөх 
) as dag:
    # 1-р үйлдэл: Wikipedia-с өгөгдлийг татах
    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "curl -f -s -S -o /opt/airflow/include/wikipageviews/wikipageviews-{{ data_interval_start.format('YYYYMMDDHH') }}.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ data_interval_start.year }}/"
            "{{ data_interval_start.year }}-{{ '{:02}'.format(data_interval_start.month) }}/"
            "pageviews-{{ data_interval_start.year }}"
            "{{ '{:02}'.format(data_interval_start.month) }}"
            "{{ '{:02}'.format(data_interval_start.day) }}-"
            "{{ '{:02}'.format(data_interval_start.hour) }}0000.gz"
        )
    )


    # 2-р үйлдэл: Татаж авсан файлыг задлах
    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="gunzip --force /opt/airflow/include/wikipageviews/wikipageviews-{{ data_interval_start.format('YYYYMMDDHH') }}.gz",
    )

    # 3-р үйлдэл: Өгөгдлийг шүүж, SQL query бэлтгэх
    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "pagenames": {
                "Google",
                "Amazon",
                "Apple",
                "Microsoft",
                "Facebook",
            }
        },
    )

    # 4-р үйлдэл: Бэлтгэсэн SQL query-г Postgres-д ажиллуулах
    write_to_postgres = SQLExecuteQueryOperator(
        task_id="write_to_postgres",
        conn_id="source_postgres  ",  # Airflow Connection-д тохируулсан холболтын нэр
        sql="postgres_query.sql", # /tmp доторх файлыг ашиглана
        return_last=False,
    )   

    # Үйлдлүүдийн ажиллах дарааллыг тодорхойлох
    get_data >> extract_gz >> fetch_pageviews >> write_to_postgres