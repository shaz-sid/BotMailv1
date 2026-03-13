#import os
#from dotenv import load_dotenv

#load_dotenv()

#GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
#EMAIL_API_KEY = os.getenv("EMAIL_API_KEY")
#DATABASE_URL = os.getenv("DATABASE_URL")
#REDIS_URL = os.getenv("REDIS_URL")
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # app
    APP_NAME:     str = "Email Outreach API"
    APP_VERSION:  str = "1.0.0"
    DEBUG:        bool = False
    PORT:         int  = 8000
    WORKERS:      int  = 4
    API_PREFIX:   str  = "/api/v1"

    # cors
    ALLOWED_ORIGINS: list[str] = ["http://localhost:3000"]

    # database
    DATABASE_URL: str

    # redis / celery
    REDIS_URL: str

    # ai
    GEMINI_API_KEY: str

    # email
    RESEND_API_KEY:      str
    EMAIL_FROM_ADDRESS:  str = "campaign@yourdomain.com"

    model_config = SettingsConfigDict(
        env_file         = ".env",
        env_file_encoding = "utf-8",
        case_sensitive   = True,
    )


settings = Settings()