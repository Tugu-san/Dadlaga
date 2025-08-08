# dags/01_download_rocket_launches.py
import json
import pathlib
import pendulum
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _get_pictures():
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests.exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests.exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")

with DAG(#Энэ бол context manager аргачлал бөгөөд энэ блок дотор тодорхойлогдсон бүх үйлдлүүд (tasks) 
        #автоматаар энэ DAG-д хамаарна гэсэн үг. Энэ нь кодыг цэгцтэй, ойлгомжтой болгодог.

    dag_id="01_download_rocket_launches", #Таны DAG-г Airflow UI дээр таниулах өвөрмөц нэр.
    start_date=pendulum.today('UTC').add(days=-14), #DAG-н ажиллагааг эхлүүлэх огноо.
    schedule=None, #Энэ DAG автоматаар ажиллахгүй, зөвхөн гараар эхлүүлнэ гэдгийг зааж байна.
) as dag:
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
        #BashOperator: bash_command-д заасан shell коммандыг ажиллуулдаг оператор. Бид curl коммандаар өгөгдлөө татаж байна.
    )
    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures,
        #PythonOperator: python_callable-д заасан Python функцийг ажиллуулдаг. 
        #Манай жишээн дээр get_pictures функцийг дуудаж, зургуудыг татаж байна.
    )
    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    )

    download_launches >> get_pictures >> notify
    #Энэ бол үйлдлүүдийн хоорондын хамаарлыг тодорхойлох хамгийн ойлгомжтой арга.
    #Энэ нь download_launches амжилттай дууссаны дараа get_pictures эхэлнэ
    #get_pictures амжилттай дууссаны дараа notify эхэлнэ гэсэн үг юм