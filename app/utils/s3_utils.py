import boto3
import os
from datetime import datetime

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def upload_to_s3(file_bytes: bytes, key: str) -> str:
    s3_client.put_object(Bucket=AWS_BUCKET_NAME, Key=key, Body=file_bytes)
    return f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"

def generate_filename_from_dates(utility_type: str, start: str, end: str) -> str:
    return f"{utility_type}_{start}_to_{end}".replace("/", "-")
