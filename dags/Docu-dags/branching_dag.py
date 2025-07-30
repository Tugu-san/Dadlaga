from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime

def choose_erp_system(**kwargs):
    # Логик: Хэрвээ өгөгдөл шинэ ERP-ээс ирсэн бол "new_erp_task", эсвэл "old_erp_task"
    data_source = check_data_source()  # Функцын логик
    if data_source == "new_erp":
        return "new_erp_task"
    else:
        return "old_erp_task"

with DAG(
    dag_id='erp_branching',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
) as dag:
  
    new_erp_task = PythonOperator(
        task_id='new_erp_task',
        python_callable=process_new_erp_data,
    )
    
    old_erp_task = PythonOperator(
        task_id='old_erp_task',
        python_callable=process_old_erp_data,
    )

    join_task = PythonOperator(
        task_id='join_task',
        python_callable=join_data,
        trigger_rule='none_failed',
    )
    
    # Энд none_failed trigger rule нь аль нэг branch амжилтгүй болсон ч join_task-ийг ажиллуулна.
    [new_erp_task, old_erp_task] >> join_task