FROM apache/airflow:latest
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow

# dags хавтас хуулах
COPY dags/ /opt/airflow/dags/