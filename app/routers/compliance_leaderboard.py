from fastapi import APIRouter, HTTPException
from dotenv import load_dotenv
import boto3
import os
import json

router = APIRouter()
load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

# Match your frontend hotel list
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

@router.get("/api/compliance/leaderboard")
def get_compliance_leaderboard():
    results = []

    for hotel in HOTELS:
        hotel_id = hotel["id"]
        key = f"{hotel_id}/compliance/latest.json"

        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            raw = obj["Body"].read().decode("utf-8")
            data = json.loads(raw)

            percent = data["percent"] if isinstance(data, dict) and "percent" in data else 0
            results.append({
                "hotel": hotel_id,  # use ID for frontend mapping/icons
                "score": round(percent)
            })
        except Exception as e:
            print(f"[WARN] Could not fetch {key}: {e}")
            results.append({
                "hotel": hotel_id,
                "score": 0
            })

    return results
