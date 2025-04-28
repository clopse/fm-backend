import os
from datetime import datetime
from uuid import uuid4
from app.core.config import settings

if settings.USE_S3:
    import boto3
    from botocore.exceptions import BotoCoreError

BASE_STORAGE_PATH = "storage"
BUCKET_NAME = "jmk-facilities"  # 🔁 replace with your actual S3 bucket name

def get_file_path(hotel_id: str, year: str, section: str, filename: str) -> str:
    """
    Returns the full file path (local or S3 key).
    If USE_S3 is enabled, uploads the file and returns the S3 key.
    """
    if settings.USE_S3:
        return f"{hotel_id}/{year}/{section}/{filename}"
    else:
        return os.path.join(BASE_STORAGE_PATH, hotel_id, year, section, filename)

def save_file(file, hotel_id: str, section: str) -> str:
    """
    Save a file (UploadFile) to local or S3.
    Returns the file path (or S3 key).
    """
    year = str(datetime.now().year)
    filename = f"{uuid4()}_{file.filename}"
    file_path = get_file_path(hotel_id, year, section, filename)

    if settings.USE_S3:
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "eu-west-1")
            )
            s3.upload_fileobj(
                file.file,
                BUCKET_NAME,
                file_path,
                ExtraArgs={"ACL": "private"}
            )
            return file_path
        except BotoCoreError as e:
            raise Exception(f"S3 upload failed: {e}")
    else:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f_out:
            f_out.write(file.file.read())
        return file_path
