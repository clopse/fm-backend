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

from app.routers.compliance_score import get_compliance_score  # Import the real score function

@router.get("/leaderboard")
def get_compliance_leaderboard():
    results = []

    for hotel in HOTELS:
        try:
            score_data = get_compliance_score(hotel["id"])  # ⬅️ Calls the actual logic
            results.append({
                "hotel": hotel["id"],
                "score": round(score_data["percent"])
            })
        except Exception as e:
            print(f"[WARN] Failed to compute score for {hotel['id']}: {e}")
            results.append({
                "hotel": hotel["id"],
                "score": 0
            })

    return results

