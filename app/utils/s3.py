import boto3
import os
import json

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "eu-west-1")
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

def save_json_to_s3(data: dict, s3_path: str):
    try:
        json_data = json.dumps(data)
        s3.put_object(Bucket=BUCKET, Key=s3_path.replace(".pdf", ".json"), Body=json_data)
    except Exception as e:
        raise RuntimeError(f"Failed to save JSON to S3: {str(e)}")
