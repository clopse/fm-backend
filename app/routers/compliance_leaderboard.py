from fastapi import APIRouter, HTTPException
from dotenv import load_dotenv
import boto3
import os
import json

router = APIRouter()
load_dotenv()

# Initialize AWS S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

HOTELS = [
    { "id": "hiex", "name": "Holiday Inn Express" },
    { "id": "moxy", "name": "Moxy Cork" },
    { "id": "hida", "name": "Holiday Inn Dublin Airport" },
    { "id": "hbhdcc", "name": "Hampton Dublin" },
    { "id": "hbhe", "name": "Hampton Ealing" },
    { "id": "sera", "name": "Seraphine" },
    { "id": "marina", "name": "Waterford Marina" },
    { "id": "hiltonth", "name": "Telephone House" },
    { "id": "belfast", "name": "Hamilton Dock" }
]

@router.get("/leaderboard")
def get_compliance_leaderboard():
    results = []

    for hotel in HOTELS:
        hotel_id = hotel["id"]
        key = f"{hotel_id}/compliance/latest.json"

        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            percent = data.get("percent", 0)
            results.append({
                "hotel": hotel_id,
                "score": round(percent)
            })
        except Exception as e:
            print(f"[WARN] Failed to load leaderboard data for {hotel_id}: {e}")
            results.append({
                "hotel": hotel_id,
                "score": 0
            })

    return results
