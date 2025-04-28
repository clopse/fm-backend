from fastapi import APIRouter, HTTPException
import os
import boto3
from dotenv import load_dotenv
from typing import Dict, List

load_dotenv()

router = APIRouter()

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
)

@router.get("/files/{hotel_id}")
def list_files(hotel_id: str) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    try:
        prefix = f"{hotel_id}/"
        response = s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)

        if "Contents" not in response:
            return {}

        reports = {
            "Service Reports": {},
            "Contracts": {}
        }

        for obj in response["Contents"]:
            key = obj["Key"]

            if key.endswith("/"):
                continue

            parts = key.split("/", 3)  # << ✅ ONLY split first 3 times
            if len(parts) < 4:
                continue

            _, top_folder, company_folder, filename = parts

            if top_folder == "reports":
                section = "Service Reports"
            elif top_folder == "contracts":
                section = "Contracts"
            else:
                continue

            if company_folder not in reports[section]:
                reports[section][company_folder] = []

            file_url = f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"

            reports[section][company_folder].append({
                "filename": filename,
                "url": file_url
            })

        return reports

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")
