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
def get_compliance_score(hotel_id: str):
    DATA_PATH = "app/data/compliance.json"
    grace_period = timedelta(days=30)
    now = datetime.utcnow()
    reports_base = f"{hotel_id}/compliance"

    try:
        with open(DATA_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    total_points = 0
    earned_points = 0
    breakdown = {}
    monthly_history = {}

    for section in sections:
        for task in section["tasks"]:
            if task["type"] != "upload":
                continue

            task_id = task["task_id"]
            points = task.get("points", 20)
            frequency = task.get("frequency", "Annually")

            total_points += points
            valid_files = []
            all_files = []

            try:
                resp = s3.list_objects_v2(Bucket=os.getenv("AWS_BUCKET_NAME"), Prefix=f"{reports_base}/{task_id}/")
                for obj in resp.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        meta = s3.get_object(Bucket=os.getenv("AWS_BUCKET_NAME"), Key=obj["Key"])
                        data = json.loads(meta["Body"].read().decode("utf-8"))
                        report_date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                        all_files.append((report_date, points))
                        if is_still_valid(frequency, report_date, now, grace_period):
                            valid_files.append(report_date)
            except Exception:
                continue

            # Task score logic
            expected_count = expected_uploads(frequency)
            actual_count = len(valid_files)
            if expected_count == 0:
                task_score = 0
            elif actual_count >= expected_count:
                task_score = points
            else:
                task_score = round(points * (actual_count / expected_count))

            breakdown[task_id] = task_score
            earned_points += task_score

            # Build monthly history
            for report_date, pts in all_files:
                month_key = report_date.strftime("%Y-%m")
                if month_key not in monthly_history:
                    monthly_history[month_key] = {"score": 0, "max": 0}
                monthly_history[month_key]["score"] += pts
                monthly_history[month_key]["max"] += pts

    return {
        "score": earned_points,
        "max_score": total_points,
        "percent": round((earned_points / total_points) * 100, 1) if total_points else 0,
        "task_breakdown": breakdown,
        "monthly_history": dict(sorted(monthly_history.items()))
    }

# -------------------------------
# ✅ Required Helper Functions
# -------------------------------
def expected_uploads(frequency: str) -> int:
    return {
        "Monthly": 12,
        "Quarterly": 4,
        "Twice Annually": 2,
        "Annually": 1,
        "Biennially": 1,
        "Every 5 Years": 1,
    }.get(frequency, 1)

def is_still_valid(frequency: str, report_date: datetime, now: datetime, grace: timedelta) -> bool:
    interval = {
        "Monthly": timedelta(days=30),
        "Quarterly": timedelta(days=90),
        "Twice Annually": timedelta(days=180),
        "Annually": timedelta(days=365),
        "Biennially": timedelta(days=730),
        "Every 5 Years": timedelta(days=5 * 365),
    }.get(frequency, timedelta(days=365))
    return (now - report_date) <= (interval + grace)

def expected_uploads(frequency: str) -> int:
    return {
        "Monthly": 12,
        "Quarterly": 4,
        "Twice Annually": 2,
        "Annually": 1,
        "Biennially": 1,
        "Every 5 Years": 1,
        "Reviewed Annually": 1,
    }.get(frequency, 1)

