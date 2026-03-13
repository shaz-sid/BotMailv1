# BotMail: AI Email Outreach Automation

An *AI-powered outreach platform* that automates contact discovery, personalised email generation, campaign sending, and engagement analytics.

This project demonstrates a *production-style backend architecture* with FastAPI, Celery workers, Redis queues, Gemini AI email generation, and a lightweight frontend dashboard.


# Features

  ### Contact Management
    - Upload contacts via CSV
    - Validate and clean contact data
    - Search and paginate contacts
    - Soft opt-out support
  
  ### AI Email Generation
    - Uses *Google Gemini API*
    - Personalised emails based on:
      - Name
      - Role
      - Company
      - Campaign description
  
  ### Campaign Management
    - Create campaigns
    - Preview AI-generated emails
    - Run campaigns asynchronously
  
  ### Distributed Email Sending
    - Celery workers handle sending
    - Redis message queue
    - Automatic retries for failures
    
  ### Campaign Analytics
  Track:
    - Emails sent
    - Failures
    - Open rate
    - Reply rate
    - Click rate
  
  ### Simple Dashboard
  Frontend built with:
    - HTML
    - CSS
    - Vanilla JavaScript



# System Architecture

  Frontend (HTML + JS)
  ↓
  FastAPI REST API
  ↓
  Services Layer
  ↓
  PostgreSQL / SQLite
  ↓
  Celery Workers
  ↓
  Redis Queue
  ↓
  Resend Email API

  AI Email Generation:
  Gemini API
  ↓
  Personalized Email Content

# Project Structure

  BotMail (email-outreach-automation)
  backend/app
  │
  ├── routes
  │ ├── analytics.py
  │ ├── campaigns.py
  │ ├── contacts.py
  │ └── email.py
  │
  ├── services
  │ ├── campaign_service.py
  │ ├── csv_service.py
  │ ├── email_service.py
  │ └── gemini_service.py
  │
  ├── workers
  │ └── celery_worker.py
  │
  ├── config.py
  ├── database.py
  ├── models.py
  ├── main.py
  │
  frontend
  ├── index.html
  ├── style.css
  └── app.js

made with <3
