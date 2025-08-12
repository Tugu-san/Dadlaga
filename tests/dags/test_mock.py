# tests/custom/test_dagtestdag.py
from airflow.providers.postgres.hooks.postgres import PostgresHook
from chapter09.dags.dagtestdag import dagtestdag # Тестлэх DAG-аа import хийнэ

# ... (pytest-docker-tools ашиглан postgres container үүсгэх fixture-г тодорхойлно)

def test_full_dag_run(mocker, postgres):
    # API дуудлагыг mock хийнэ
    mocker.patch.object(MovielensHook, "get_ratings", return_value=[...])

    # PostgresHook-н холболтыг Docker доторх Postgres руу чиглүүлнэ
    mocker.patch.object(
        PostgresHook, "get_connection",
        return_value=Connection(..., port=postgres.ports["5432/tcp"][0])
    )
    
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    
    # 1. Ажиллуулахаас өмнө хүснэгт хоосон байгааг шалгана
    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count == 0
    
    # 2. Бүтэн DAG-г ажиллуулна!
    dagtestdag.test()
    
    # 3. Ажиллаж дууссаны дараа хүснэгтэд өгөгдөл орсон эсэхийг шалгана
    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count > 0