from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import time

# Импорт твоих функций из load_ria_ua.py
from load_ria_ua import load_trailers_by_bodystyle, load_trailers_by_bodystyle, get_mongo_collection

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Список bodystyles для трейлеров и полуприцепов
TRAILER_BODYSTYLES = [26, 27, 28, 29]
SEMITRAILER_BODYSTYLES = [39, 40, 41]

# Периоды загрузки — 5 кварталов, начиная с 2023 Q4
PERIODS = [
    (2023, 4),
    (2024, 1),
    (2024, 2),
    (2024, 3),
    (2024, 4),
]

def wait_75_min():
    print("⏳ Ждём 75 минут, чтобы не превышать лимиты API")
    time.sleep(75 * 60)

def load_trailers_task(year, quarter, **kwargs):
    coll = get_mongo_collection("trailers")
    log_coll = get_mongo_collection("logs")
    load_trailers_by_bodystyle(
        collection=coll,
        bodystyles=TRAILER_BODYSTYLES,
        group_name="trailers",
        year=year,
        quarter=quarter,
        log_col=log_coll,
        max_pages=3,
        delay=0.5
    )

def load_semitrailers_task(year, quarter, **kwargs):
    coll = get_mongo_collection("semitrailers")
    log_coll = get_mongo_collection("logs")
    load_trailers_by_bodystyle(
        collection=coll,
        bodystyles=SEMITRAILER_BODYSTYLES,
        group_name="semitrailers",
        year=year,
        quarter=quarter,
        log_col=log_coll,
        max_pages=3,
        delay=0.5
    )


with DAG(
    dag_id="ria_trailers_5_quarters_load",
    default_args=default_args,
    description="Load trailers and semitrailers data for 5 quarters starting from 2023 Q4",
    start_date=datetime(2025, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ria", "trailers", "load"],
) as dag:

    load_tasks = []

    for i, (year, quarter) in enumerate(PERIODS):
        load_trailers = PythonOperator(
            task_id=f"load_trailers_{year}_q{quarter}",
            python_callable=load_trailers_task,
            op_kwargs={"year": year, "quarter": quarter},
        )

        load_semitrailers = PythonOperator(
            task_id=f"load_semitrailers_{year}_q{quarter}",
            python_callable=load_semitrailers_task,
            op_kwargs={"year": year, "quarter": quarter},
        )

        load_tasks.append((load_trailers, load_semitrailers))

    wait_tasks = []

    for i, (trailer_task, semitrailer_task) in enumerate(load_tasks):
        # Сначала трейлеры, потом полуприцепы в одном квартале
        trailer_task >> semitrailer_task

        # Между кварталами вставляем паузу в 75 минут, чтобы не превышать API лимиты
        if i < len(load_tasks) - 1:
            wait_task = PythonOperator(
                task_id=f"wait_75_min_after_q{i+1}",
                python_callable=wait_75_min,
            )
            wait_tasks.append(wait_task)

            semitrailer_task >> wait_task
            wait_task >> load_tasks[i + 1][0]

