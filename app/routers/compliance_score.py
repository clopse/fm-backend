# /app/routers/compliance_score.py
from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
import boto3

router = APIRouter()
load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")
RULES_PATH = "app/data/compliance.json"

@router.get("/api/compliance/score/{hotel_id}")
async def get_compliance_score(hotel_id: str):
    try:
        with open(RULES_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    now = datetime.utcnow()
    score = 0
    max_score = 0
    detailed = []
    history = {}  # Format: {"2025-01": {"score": x, "max": y}}

    for section in sections:
        for task in section["tasks"]:
            task_id = task["task_id"]
            points = task.get("points", 0)
            freq = task["frequency"].lower()
            period_days = {
                "daily": 1,
                "weekly": 7,
                "monthly": 30,
                "quarterly": 90,
                "twice annually": 180,
                "annually": 365,
                "biennially": 730,
                "every 5 years": 1825
            }.get(freq, 0)

            if task["type"] == "confirmation":
                # Future: handle checkbox history here
                max_score += points
                continue

            prefix = f"{hotel_id}/compliance/{task_id}/"
            files = []

            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
                for obj in resp.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                        data = json.loads(meta["Body"].read().decode("utf-8"))
                        date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                        files.append(date)
            except:
                pass

            files.sort(reverse=True)

            # Allocate points if report is within valid period (plus 1 month grace)
            if files:
                latest = files[0]
                valid_until = latest + timedelta(days=period_days + 30)
                if now <= valid_until:
                    score += points

            max_score += points

            # Monthly breakdown
            for fdate in files:
                month_key = fdate.strftime("%Y-%m")
                if month_key not in history:
                    history[month_key] = {"score": 0, "max": 0}
                history[month_key]["score"] += points
                history[month_key]["max"] += points

            detailed.append({
                "task_id": task_id,
                "label": task["label"],
                "scored": points if files and now <= valid_until else 0,
                "max": points,
                "valid_until": valid_until.strftime("%Y-%m-%d") if files else None
            })

    return {
        "score": score,
        "max_score": max_score,
        "percent": round(score / max_score * 100, 1) if max_score else 0,
        "detailed": detailed,
        "monthly_history": history
    }
