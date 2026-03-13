import imaplib
import email
from config import *

def connect_imap():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    mail.select("inbox")
    return mail

def fetch_unread_emails(mail):
    status, messages = mail.search(None, '(UNSEEN)')
    email_ids = messages[0].split()

    emails = []
    for eid in email_ids:
        _, data = mail.fetch(eid, '(RFC822)')
        msg = email.message_from_bytes(data[0][1])
        emails.append((eid, msg))

    return emails

def extract_email_data(msg):
    subject = msg.get("Subject", "")
    sender = msg.get("From", "")
    return subject, sender
