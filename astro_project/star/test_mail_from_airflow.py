from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='test_send_email',
    default_args=default_args,
    description='Test email sending DAG',
    start_date=datetime(2025, 7, 4),
    schedule=None,
    catchup=False,
) as dag:

    def send_test_email():
        smtp_user = Variable.get("SMTP_USER")
        smtp_password = Variable.get("EMAIL_PASSWORD")
        recipient = Variable.get("EMAIL_RECIPIENT")
        sender = Variable.get("EMAIL_SENDER")

        if not smtp_user or not smtp_password or not recipient or not sender:
            raise ValueError("One or more Airflow Variables missing: SMTP_USER, EMAIL_PASSWORD, EMAIL_RECIPIENT, EMAIL_SENDER")

        subject = "Тестовое письмо от Airflow"
        body = "Всё работает!"

        message = MIMEText(body, "plain")
        message["From"] = sender
        message["To"] = recipient
        message["Subject"] = subject

        try:
            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(sender, recipient, message.as_string())
            server.quit()
            print(f"✅ Тестовое письмо успешно отправлено на {recipient}")
        except Exception as e:
            print(f"❌ Ошибка при отправке письма: {e}")
            raise

    task_send_email = PythonOperator(
        task_id='send_test_email',
        python_callable=send_test_email,
    )
