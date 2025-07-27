!pip install dotenv

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# подгружаем .env
load_dotenv()

def send_test_email():
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    recipient = os.getenv("EMAIL_RECIPIENT")
    
    if not smtp_user or not smtp_password:
        print("❌ SMTP_USER или SMTP_PASSWORD не найдены в переменных окружения")
        return
    
    if not recipient:
        print("❌ EMAIL_RECIPIENT не найден в переменных окружения")
        return
    
    smtp_host = "smtp.gmail.com"
    smtp_port = 587
    
    msg = MIMEMultipart()
    msg["From"] = smtp_user
    msg["To"] = recipient
    msg["Subject"] = "✅ Airflow test email"
    
    body = "Привет! Это тестовое письмо от Airflow. Все ок 👍"
    msg.attach(MIMEText(body, "plain"))
    
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient, msg.as_string())
        server.quit()
        print(f"✅ Test email sent successfully to {recipient}")
    except Exception as e:
        print(f"❌ Failed to send test email: {e}")

# запуск
send_test_email()
