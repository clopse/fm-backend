# ✅ FILE: app/utils/storage.py

import os
from datetime import datetime
from uuid import uuid4
from app.core.config import settings

if settings.USE_S3:
    import boto3
    from botocore.exceptions import BotoCoreError

BASE_STORAGE_PATH = "storage"
BUCKET_NAME = "jmk-project-uploads"


def get_file_path(hotel_id: str, year: str, section: str, filename: str) -> str:
    if settings.USE_S3:
        return f"{hotel_id}/{year}/{section}/{filename}"
    else:
        return os.path.join(BASE_STORAGE_PATH, hotel_id, year, section, filename)


def save_file(file_or_bytes, hotel_id: str, section: str, filename: str = None) -> str:
    year = str(datetime.now().year)
    filename = filename or f"{uuid4()}"
    file_path = get_file_path(hotel_id, year, section, filename)

    if settings.USE_S3:
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_REGION", "eu-west-1")
            )

            if hasattr(file_or_bytes, "file"):  # UploadFile
                s3.upload_fileobj(
                    file_or_bytes.file,
                    BUCKET_NAME,
                    file_path,
                    ExtraArgs={"ACL": "private"}
                )
            else:  # BytesIO or raw stream
                s3.upload_fileobj(
                    file_or_bytes,
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
            content = file_or_bytes.file.read() if hasattr(file_or_bytes, "file") else file_or_bytes.read()
            f_out.write(content)
        return file_path
