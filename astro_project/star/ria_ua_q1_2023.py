from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Добавляем путь к include, где лежит load_ria_ua.py
sys.path.append(os.path.join(os.path.dirname(__file__), "../include"))

from load_ria_ua import load_trailers_by_bodystyle, get_mongo_collection

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="ria_ua_q1_2023",
    default_args=default_args,
    description="Load RIA UA trailers and semitrailers for Q1 2023",
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    tags=["ria", "ukraine", "q1", "2023"],
) as dag:

    def load_trailers_q1_2023():
        trailers_col = get_mongo_collection("ua_trailer_raw")
        log_col = get_mongo_collection("ria_ua_load_logs")
        trailers_bodystyles = [147, 148, 149, 150, 151, 152, 153, 154, 155, 157, 159, 160]
        load_trailers_by_bodystyle(
            trailers_col,
            trailers_bodystyles,
            group_name="trailers",
            year=2023,
            quarter=1,
            month_range=(1, 3),
            log_col=log_col
        )

    def load_semitrailers_q1_2023():
        semitrailers_col = get_mongo_collection("ria_semitrailers_raw")
        log_col = get_mongo_collection("ria_ua_load_logs")
        semitrailers_bodystyles = [161, 162, 163, 164, 165, 167, 168, 169, 170, 171, 172, 173, 175, 176, 177, 178]
        load_trailers_by_bodystyle(
            semitrailers_col,
            semitrailers_bodystyles,
            group_name="semitrailers",
            year=2023,
            quarter=1,
            month_range=(1, 3),
            log_col=log_col
        )

    task_trailers = PythonOperator(
        task_id="load_trailers_q1_2023",
        python_callable=load_trailers_q1_2023,
    )

    task_semitrailers = PythonOperator(
        task_id="load_semitrailers_q1_2023",
        python_callable=load_semitrailers_q1_2023,
    )

    task_trailers >> task_semitrailers
