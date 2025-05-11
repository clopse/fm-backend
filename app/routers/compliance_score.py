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

@router.get("/score/{hotel_id}")
def get_compliance_score(hotel_id: str):
    grace_period = timedelta(days=30)
    now = datetime.utcnow()

    try:
        with open(RULES_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    total_points = 0
    earned_points = 0
    breakdown = {}
    monthly_history = {}

    for section in sections:
        for task in section["tasks"]:
            task_id = task["task_id"]
            task_type = task.get("type", "upload")
            frequency = task.get("frequency", "Annually")
            points = task.get("points", 20)
            total_points += points

            if task_type == "upload":
                valid_files = []
                all_files = []
                try:
                    prefix = f"{hotel_id}/compliance/{task_id}/"
                    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
                    for obj in resp.get("Contents", []):
                        if obj["Key"].endswith(".json"):
                            meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                            data = json.loads(meta["Body"].read().decode("utf-8"))
                            report_date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                            all_files.append((report_date, points))
                            if is_still_valid(frequency, report_date, now, grace_period):
                                valid_files.append(report_date)
                except Exception:
                    pass

                expected_count = expected_uploads(frequency)
                actual_count = len(valid_files)
                score = round(points * (actual_count / expected_count)) if expected_count else 0
                score = min(score, points)

                earned_points += score
                breakdown[task_id] = score

                for report_date, pts in all_files:
                    mkey = report_date.strftime("%Y-%m")
                    if mkey not in monthly_history:
                        monthly_history[mkey] = {"score": 0, "max": 0}
                    monthly_history[mkey]["score"] += pts
                    monthly_history[mkey]["max"] += pts

            elif task_type == "confirmation":
                latest = None
                try:
                    prefix = f"{hotel_id}/confirmations/{task_id}/"
                    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
                    for obj in resp.get("Contents", []):
                        if obj["Key"].endswith(".json"):
                            meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                            data = json.loads(meta["Body"].read().decode("utf-8"))

                            # Use either report_date or confirmed_at
                            date_str = data.get("report_date") or data.get("confirmed_at")
                            if not date_str:
                                continue
                            report_date = datetime.strptime(date_str[:10], "%Y-%m-%d")

                            if not latest or report_date > latest:
                                latest = report_date
                except Exception:
                    pass

                if latest and is_still_valid(frequency, latest, now, grace_period):
                    earned_points += points
                    breakdown[task_id] = points
                    mkey = latest.strftime("%Y-%m")
                    if mkey not in monthly_history:
                        monthly_history[mkey] = {"score": 0, "max": 0}
                    monthly_history[mkey]["score"] += points
                    monthly_history[mkey]["max"] += points
                else:
                    breakdown[task_id] = 0

    return {
        "score": earned_points,
        "max_score": total_points,
        "percent": round((earned_points / total_points) * 100, 1) if total_points else 0,
        "task_breakdown": breakdown,
        "monthly_history": dict(sorted(monthly_history.items()))
    }


# --- Helpers ---

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

def is_still_valid(frequency: str, report_date: datetime, now: datetime, grace: timedelta) -> bool:
    interval = {
        "Monthly": timedelta(days=30),
        "Quarterly": timedelta(days=90),
        "Twice Annually": timedelta(days=180),
        "Annually": timedelta(days=365),
        "Biennially": timedelta(days=730),
        "Every 5 Years": timedelta(days=1825),
    }.get(frequency, timedelta(days=365))

    return (now - report_date) <= (interval + grace)
