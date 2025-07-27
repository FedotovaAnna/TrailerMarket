from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import smtplib
from email.mime.text import MIMEText
from pymongo import MongoClient
from dotenv import load_dotenv

# dotenv для Airflow, если нужно
load_dotenv()

# добавляем include
sys.path.append(os.path.join(os.path.dirname(__file__), "../include"))
from load_ria_ua import load_trailers_by_bodystyle, get_mongo_collection

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="ria_ua_q2_2023_notify",
    default_args=default_args,
    description="Load RIA UA trailers/semitrailers Q2 2023 + email notification",
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["ria", "ukraine", "q2", "2023"],
) as dag:

    def load_trailers_q2_2023():
        trailers_col = get_mongo_collection("ua_trailer_raw")
        log_col = get_mongo_collection("ria_ua_load_logs")
        trailers_bodystyles = [147, 148, 149, 150, 151, 152, 153, 154, 155, 157, 159, 160]
        load_trailers_by_bodystyle(
            trailers_col,
            trailers_bodystyles,
            group_name="trailers",
            year=2023,
            quarter=2,
            month_range=(4, 6),
            log_col=log_col
        )

    def load_semitrailers_q2_2023():
        semitrailers_col = get_mongo_collection("ria_semitrailers_raw")
        log_col = get_mongo_collection("ria_ua_load_logs")
        semitrailers_bodystyles = [161, 162, 163, 164, 165, 167, 168, 169, 170, 171, 172, 173, 175, 176, 177, 178]
        load_trailers_by_bodystyle(
            semitrailers_col,
            semitrailers_bodystyles,
            group_name="semitrailers",
            year=2023,
            quarter=2,
            month_range=(4, 6),
            log_col=log_col
        )

    def send_email_summary():
        smtp_user = os.getenv("SMTP_USER")
        smtp_password = os.getenv("SMTP_PASSWORD")
        recipient = os.getenv("EMAIL_RECIPIENT")
        
        if not smtp_user or not smtp_password or not recipient:
            raise ValueError("SMTP_USER, SMTP_PASSWORD, or EMAIL_RECIPIENT missing in environment variables")
        
        # читаем последний лог
        uri = f"mongodb+srv://afedotova1974:{os.environ['db_password']}@cluster0.le9nhy1.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client = MongoClient(uri)
        log_col = client["trailer"]["ria_ua_load_logs"]
        
        # последние два лога за квартал 2 2023
        logs = log_col.find({"year": 2023, "quarter": 2}).sort("loaded_at", -1).limit(2)
        
        text_lines = []
        for log in logs:
            text_lines.append(
                f"{log['group_name'].capitalize()}: {log['total_loaded']} записей за {log['elapsed_seconds']:.1f} секунд"
            )
        text_body = "\n".join(text_lines) if text_lines else "Нет логов для отчета."
        
        subject = "✅ Airflow загрузка RIA_UA Q2 2023 завершена"
        
        message = MIMEText(text_body, "plain")
        message["From"] = smtp_user
        message["To"] = recipient
        message["Subject"] = subject
        
        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipient, message.as_string())
            server.quit()
            print(f"✅ Email summary sent to {recipient}")
        except Exception as e:
            print(f"❌ Failed to send email: {e}")

    task_trailers = PythonOperator(
        task_id="load_trailers_q2_2023",
        python_callable=load_trailers_q2_2023,
    )

    task_semitrailers = PythonOperator(
        task_id="load_semitrailers_q2_2023",
        python_callable=load_semitrailers_q2_2023,
    )

    task_email = PythonOperator(
        task_id="send_email_summary",
        python_callable=send_email_summary,
    )

    task_trailers >> task_semitrailers >> task_email
