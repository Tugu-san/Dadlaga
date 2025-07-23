import pandas as pd

from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id="Merged_1212_data",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["1212", "Data Extraction"]
)
def dag_with_merged_1212_data():

    @task()
    def extract_data():
        infl = pd.read_csv('/opt/airflow/include/1212-data/CONT-5 Инфляцийн түвшин (хэрэглээний үнийн индекс).csv')
        ajilgui_data = pd.read_csv('/opt/airflow/include/1212-data/Ажилгүйдлийн түвшин.csv')
        urh_orlogo = pd.read_csv('/opt/airflow/include/1212-data/НЭГ ӨРХИЙН САРЫН ДУНДАЖ ОРЛОГО.csv')
        cpi_index1 = pd.read_csv('/opt/airflow/include/1212-data/ХЭРЭГЛЭЭНИЙ ҮНИЙН БҮСИЙН ИНДЕКС.csv')
        
        return {
        "infl": infl,
        "ajilgui_data": ajilgui_data,
        "urh_orlogo": urh_orlogo,
        "cpi_index1": cpi_index1
    }
    
    @task()
    def transform_data(data):

        infl = data["infl"]
        ajilgui_data = data["ajilgui_data"]
        urh_orlogo = data["urh_orlogo"]
        cpi_index1 = data["cpi_index1"]

        urh_orlogo.rename(columns={'2024': 'Нэг өрхийн сарын дундаж орлого'}, inplace=True)
        urh_orlogo.rename(columns={'Бүс': 'Бүс нутаг'}, inplace=True)

        infl.rename(columns={'2024': 'Инфляц'}, inplace=True)
        infl.rename(columns={'ангилал': 'Бүс нутаг'}, inplace=True)
        infl = infl[infl['Бүс нутаг'] != 'Улсын дүн']
        
        infl['Бүс нутаг'] = infl['Бүс нутаг'].replace('Баруун', 'Баруун бүс')
        infl['Бүс нутаг'] = infl['Бүс нутаг'].replace('Хангай', 'Хангайн бүс')
        infl['Бүс нутаг'] = infl['Бүс нутаг'].replace('Төв', 'Төвийн бүс')
        infl['Бүс нутаг'] = infl['Бүс нутаг'].replace('Зүүн', 'Зүүн бүс')

        ajilgui_data.rename(columns={'2024': 'Ажилгүйдэл түвшин'}, inplace=True)
        ajilgui_data.rename(columns={'Ангилал': 'Бүс нутаг'}, inplace=True)
        ajilgui_data['Бүс нутаг'] = ajilgui_data['Бүс нутаг'].replace('Бүгд', 'Улсын дүн')
        ajilgui_data = ajilgui_data[ajilgui_data['Бүс нутаг'] != 'Улсын дүн']

        Infl_ajilgui_data = infl.merge(ajilgui_data, on='Бүс нутаг', how='left')

        cpi_index1.drop(columns=['Статистик үзүүлэлт'], inplace=True)
        cpi_index1.drop(columns=['БҮЛЭГ'], inplace=True)
        cpi_index1.rename(columns={'2024-12': 'Хэрэглээний үнийн бүсийн индекс'}, inplace=True)
        cpi_index1.rename(columns={'Бүс': 'Бүс нутаг'}, inplace=True)

        cpi_index1 = pd.concat([
            cpi_index1,
            pd.DataFrame([{'Бүс нутаг': 'Улаанбаатар', 'Хэрэглээний үнийн бүсийн индекс': 8.3}])
        ], ignore_index=True)

        merged = urh_orlogo.merge(Infl_ajilgui_data, on='Бүс нутаг', how='left')
        merged = merged.merge(cpi_index1, on='Бүс нутаг', how='left')

        return merged
    
    @task()
    def save_to_parquet(merged):
        merged.to_parquet('/opt/airflow/include/1212-data/1212_merged_data.parquet', index=False)
        return '/opt/airflow/include/1212-data/1212_merged_data.parquet'
    
    data = extract_data()
    transformed_data = transform_data(data)
    save_to_parquet = save_to_parquet(transformed_data)

dag_with_merged_1212_data = dag_with_merged_1212_data()