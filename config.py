mport os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # === Email Config ===
    EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.blacknight.com")
    EMAIL_PORT = int(os.getenv("EMAIL_PORT", 587))
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")

    # === Auth ===
    JWT_SECRET = os.getenv("JWT_SECRET", "default-secret")
    TOKEN_EXPIRY_MINUTES = int(os.getenv("TOKEN_EXPIRY_MINUTES", 60))

    # === API URLs ===
    FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
    BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

    # === S3 Config ===
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")

    # === Feature Toggles ===
    USE_S3 = os.getenv("USE_S3", "false").lower() == "true"

    # === Optional DB ===
    DB_URL = os.getenv("DB_URL", "")

settings = Settings()
