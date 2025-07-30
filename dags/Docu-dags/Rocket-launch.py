from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json, requests, pathlib
import pendulum

with DAG(
    dag_id="01_download_rocket_launches",           #UI-д харагдах нэр
    start_date=pendulum.today('UTC').add(days=-1),  #эхлэх огноо
    schedule=None                                   #зөвхөн гараар ажиллана
) as dag:

    #Энэ task JSON файл татаж /tmp/launches.json дээр хадгална.
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming' -o /tmp/launches.json"
    )

    #JSON дотроос "image" талбаруудыг авч зургуудыг татдаг функц.
    def _get_pictures():
        pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
        with open("/tmp/launches.json") as f:
            launches = json.load(f)
            for launch in launches["results"]:
                url = launch["image"]
                try:
                    img_data = requests.get(url).content
                    filename = url.split("/")[-1]
                    with open(f"/tmp/images/{filename}", "wb") as img_file:
                        img_file.write(img_data)
                except:
                    print(f"Failed to download {url}")

    #2-р task: PythonOperator 
    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures
    )

    #3-р task: Notify
    notify = BashOperator(
        task_id="notify",
        bash_command="echo 'There are now $(ls /tmp/images | wc -l) images.'"
    )

    # Task-ийн дарааллыг заах
    download_launches >> get_pictures >> notify



