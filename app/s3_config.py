# backend/app/s3_config.py
import os
import boto3
import json
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

# ADD THIS FUNCTION:
def get_hotel_compliance_tasks(hotel_id: str):
    """Load hotel-specific compliance tasks from S3"""
    try:
        key = f"hotels/facilities/{hotel_id}tasks.json"
        response = s3.get_object(Bucket=BUCKET, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Extract and flatten tasks
        all_tasks = []
        for section in data.get("complianceData", []):
            for task in section.get("tasks", []):
                all_tasks.append(task)
        
        return all_tasks
    except s3.exceptions.NoSuchKey:
        print(f"[INFO] No compliance file found for hotel {hotel_id}")
        return []  # Hotel file doesn't exist = 0 score
    except Exception as e:
        print(f"[ERROR] Error loading compliance for {hotel_id}: {e}")
        return []
