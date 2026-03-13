import smtplib
from email.mime.text import MIMEText
from config import *

def send_auto_reply(receiver, subject):
    msg = MIMEText(AUTO_REPLY_MESSAGE)

    msg["From"] = EMAIL_ADDRESS
    msg["To"] = receiver
    msg["Subject"] = f"Re: {subject}"

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    server.send_message(msg)
    server.quit()
