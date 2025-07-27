import sys
from datetime import datetime, timedelta

# Добавляем папку include в sys.path
sys.path.append("/usr/local/airflow/include")

from airflow import DAG
from airflow.operators.python import PythonOperator
from load_ria_ua import load_trailers_by_bodystyle, get_mongo_collection


def test_import_func():
    print("✅ Импорт из load_ria_ua успешен!")
    # Проверяем получение коллекции (не делаем загрузку, чтобы тест был быстрым)
    col = get_mongo_collection("ria_semitrailers_raw")
    print(f"Получена коллекция: {col.name}")


default_args = {
    "owner": "Anna",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="test_load_ria_ua_import",
    default_args=default_args,
    catchup=False,
    schedule=None,  # ручной запуск
    tags=["test", "import"],
) as dag:

    test_import_task = PythonOperator(
        task_id="test_import_task",
        python_callable=test_import_func,
    )
