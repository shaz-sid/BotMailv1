def move_email(mail, email_id, folder_name):
    mail.create(folder_name)
    mail.copy(email_id, folder_name)
    mail.store(email_id, '+FLAGS', '\\Deleted')
