import re

def has_word(text, word):
    return re.search(rf"\b{re.escape(word)}\b", text) is not None


def classify_email(subject, sender):
    subject = subject.lower()
    sender = sender.lower()

    # ---------- STRONG SIGNALS ----------

    # College indicators
    college_keywords = [
        "exam", "assignment", "semester", "university",
        "college", "lecture", "result", "submission",
        "timetable", "professor", "faculty"
    ]

    college_domains = [
        ".edu", ".ac.", "university", "college"
    ]

    # Work indicators
    work_keywords = [
        "job", "career", "interview", "offer",
        "recruitment", "hiring", "hr", "joining",
        "salary", "position", "role"
    ]

    work_domains = [
        "hr@", "careers@", "jobs@", "recruit",
        "company", "corp", "ltd", "inc"
    ]

    # Spam indicators (VERY EXPLICIT ONLY)
    spam_phrases = [
        "win money", "lottery", "free prize",
        "urgent action", "limited time offer",
        "click here", "act now", "earn fast",
        "work from home and earn", "guaranteed income"
    ]

    # ---------- CLASSIFICATION ORDER ----------

    # 1️⃣ COLLEGE
    if any(has_word(subject, k) for k in college_keywords):
        return "College"

    if any(d in sender for d in college_domains):
        return "College"

    # 2️⃣ WORK
    if any(has_word(subject, k) for k in work_keywords):
        return "Work"

    if any(d in sender for d in work_domains):
        return "Work"

    # 3️⃣ SPAM (ONLY IF VERY CLEAR)
    if any(p in subject for p in spam_phrases):
        return "Spam"

    # 4️⃣ GENERAL (SAFE DEFAULT)
    return "General"
