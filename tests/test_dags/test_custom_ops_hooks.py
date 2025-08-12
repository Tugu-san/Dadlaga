# tests/custom/test_movielens_popularity_operator.py
from custom.movielens_popularity_operator import MovielensPopularityOperator
from custom.movielens_hook import MovielensHook
from airflow.models import Connection

def test_movielens_popularity_operator(mocker):
    """MovielensPopularityOperator-н логикийг шалгана."""
    
    # 1. MovielensHook-н get_connection методын дуудлагыг mock хийнэ.
    # Энэ нь Airflow-н metastore-с холболт хайхын оронд
    # бидний заасан хуурамч Connection объектыг буцаана.
    mocker.patch.object(
        MovielensHook,
        "get_connection",
        return_value=Connection(conn_id="test", login="airflow", password="airflow"),
    )

    # 2. API-с ирэх үр дүнг mock хийнэ.
    mocker.patch.object(
        MovielensHook,
        "get_ratings",
        return_value=[
            {"movieId": 1, "rating": 5},
            {"movieId": 1, "rating": 4},
            {"movieId": 2, "rating": 3},
        ]
    )

    # Операторыг үүсгэж, execute методын ажиллуулна
    task = MovielensPopularityOperator(
        task_id="test_id",
        conn_id="test",
        start_date="2025-01-01",
        end_date="2025-01-02",
        top_n=5,
    )
    result = task.execute(context={})

    # Үр дүнгээ шалгана
    assert len(result) > 0
    assert result[0][0] == 1  # movieId 1-н дундаж үнэлгээ илүү өндөр байх ёстой