import boto3
import os
import json
from datetime import datetime

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "eu-west-1")
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

def save_json_to_s3(data: dict, hotel_id: str, utility_type: str, billing_start: str, original_filename: str) -> str:
    try:
        year = billing_start.split("-")[0]
        sanitized_filename = original_filename.replace(" ", "_").replace(".pdf", ".json")
        s3_path = f"{hotel_id}/{utility_type}/{year}/{sanitized_filename}"

        json_data = json.dumps(data)
        s3.put_object(Bucket=BUCKET, Key=s3_path, Body=json_data)

        return s3_path
    except Exception as e:
        raise RuntimeError(f"Failed to save JSON to S3: {str(e)}")

def save_pdf_to_s3(content: bytes, hotel_id: str, utility_type: str, billing_start: str, original_filename: str) -> str:
    try:
        year = billing_start.split("-")[0]
        s3_path = f"{hotel_id}/{utility_type}/{year}/{original_filename}"
        s3.put_object(Bucket=BUCKET, Key=s3_path, Body=content)
        return s3_path
    except Exception as e:
        raise RuntimeError(f"Failed to save PDF to S3: {str(e)}")
