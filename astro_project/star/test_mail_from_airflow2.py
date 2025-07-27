from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from load_ria_ua import load_trailers_by_bodystyle, get_mongo_collection, send_email_summary

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Пакеты bodystyles для трейлеров и семитрейлеров (пример)
trailers_bodystyles = [1001, 1002]       # Заменить на реальные ID
semitrailers_bodystyles = [2001, 2002]   # Заменить на реальные ID

with DAG(
    dag_id="ria_trailers_batch_load",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ria", "batch", "trailers"],
) as dag:

    log_collection = get_mongo_collection("load_logs")
    data_collection = get_mongo_collection("trailers_data")

    def load_and_email(year, quarter, group_name, bodystyles):
        print(f"--- Запуск загрузки: {group_name}, {year} Q{quarter} ---")
        load_trailers_by_bodystyle(
            collection=data_collection,
            bodystyles=bodystyles,
            group_name=group_name,
            year=year,
            quarter=quarter,
            log_col=log_collection,
            max_pages=5,    # например
            delay=0.5
        )
        subject = f"Отчет загрузки {group_name} {year} Q{quarter}"
        body = f"Загрузка {group_name} за {year} квартал {quarter} завершена."
        send_email_summary(subject, body)

    def wait_75_min():
        import time
        print("Пауза 1 час 15 минут...")
        time.sleep(75 * 60)  # 75 минут в секундах

    load_q4_2023_trailers = PythonOperator(
        task_id="load_q4_2023_trailers",
        python_callable=load_and_email,
        op_kwargs={"year": 2023, "quarter": 4, "group_name": "trailers", "bodystyles": trailers_bodystyles},
    )

    load_q4_2023_semitrailers = PythonOperator(
        task_id="load_q4_2023_semitrailers",
        python_callable=load_and_email,
        op_kwargs={"year": 2023, "quarter": 4, "group_name": "semitrailers", "bodystyles": semitrailers_bodystyles},
    )

    wait_after_q4_2023 = PythonOperator(
        task_id="wait_75_min",
        python_callable=wait_75_min,
    )

    load_q1_2024_trailers = PythonOperator(
        task_id="load_q1_2024_trailers",
        python_callable=load_and_email,
        op_kwargs={"year": 2024, "quarter": 1, "group_name": "trailers", "bodystyles": trailers_bodystyles},
    )

    load_q1_2024_semitrailers = PythonOperator(
        task_id="load_q1_2024_semitrailers",
        python_callable=load_and_email,
        op_kwargs={"year": 2024, "quarter": 1, "group_name": "semitrailers", "bodystyles": semitrailers_bodystyles},
    )

    # Определяем порядок выполнения:
    load_q4_2023_trailers >> load_q4_2023_semitrailers >> wait_after_q4_2023 >> load_q1_2024_trailers >> load_q1_2024_semitrailers
