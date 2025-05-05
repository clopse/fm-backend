# app/config.py or app/settings.py

import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # === DocuPanda ===
    DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
    SCHEMA_ELECTRICITY = "3ca991a9"
    SCHEMA_GAS = "bd3ec499"

    # === Email ===
    EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.blacknight.com")
    EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")

    # === Auth ===
    JWT_SECRET = os.getenv("JWT_SECRET", "default-secret")
    TOKEN_EXPIRY_MINUTES = int(os.getenv("TOKEN_EXPIRY_MINUTES", 60))

    # === Frontend & Backend URLs ===
    FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
    BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

    # === S3 ===
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
    USE_S3 = os.getenv("USE_S3", "false").lower() == "true"

    # === Optional DB ===
    DB_URL = os.getenv("DB_URL", "")

settings = Settings()
