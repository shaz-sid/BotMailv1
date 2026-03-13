from email_reader import connect_imap, fetch_unread_emails, extract_email_data
from classifier import classify_email
from email_mover import move_email
from auto_responder import send_auto_reply

def main():
    mail = connect_imap()
    emails = fetch_unread_emails(mail)

    print(f"Unread emails found: {len(emails)}")
    print("-" * 50)

    for eid, msg in emails:
        subject, sender = extract_email_data(msg)

       
        category = classify_email(subject, sender)

        # DEBUG 
        print("SUBJECT :", subject)
        print("SENDER  :", sender)
        print("CATEGORY:", category)
        print("-" * 50)

        #  Move email
        move_email(mail, eid, category)

        # Auto-reply ONLY if not spam
        if category != "Spam":
            send_auto_reply(sender, subject)

    mail.expunge()
    mail.logout()

if __name__ == "__main__":
    main()
