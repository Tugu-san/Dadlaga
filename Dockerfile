FROM apache/airflow:latest
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install apache-airflow-providers-sftp apache-airflow-providers-postgres pandas SQLAlchemy