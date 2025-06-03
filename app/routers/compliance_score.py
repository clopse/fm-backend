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

@router.get("/score/{hotel_id}")
def get_compliance_score(hotel_id: str):
    grace_period = timedelta(days=30)
    now = datetime.utcnow()

    try:
        from app.s3_config import get_hotel_compliance_tasks
        all_tasks = get_hotel_compliance_tasks(hotel_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    total_points = 0
    earned_points = 0
    breakdown = {}
    monthly_history = {}

    for task in all_tasks:
        task_id = task["task_id"]
        task_type = task.get("type", "upload")
        frequency = task.get("frequency", "Annually")
        points = task.get("points", 20)
        total_points += points

        if task_type == "upload":
            seen_dates = set()
            valid_dates = set()
            valid_files = []

            try:
                prefix = f"{hotel_id}/compliance/{task_id}/"
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
                for obj in resp.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                        data = json.loads(meta["Body"].read().decode("utf-8"))
                        report_date_str = data.get("report_date")
                        if not report_date_str:
                            continue

                        report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
                        upload_date = obj["LastModified"]

                        # Avoid duplicates by report_date
                        if report_date_str not in seen_dates:
                            seen_dates.add(report_date_str)
                            if report_date >= now - timedelta(days=365):
                                valid_dates.add(report_date_str)
                                # Only add to valid_files if it's still valid
                                if is_still_valid(frequency, report_date, now, grace_period):
                                    valid_files.append((report_date, upload_date, points))
            except Exception:
                pass

            expected_count = expected_uploads(frequency)
            actual_count = len(valid_dates)
            score = round(points * (actual_count / expected_count)) if expected_count else 0
            score = min(score, points)

            earned_points += score
            breakdown[task_id] = score

            # Only track uploads that are still valid in monthly history
            for report_date, upload_date, pts in valid_files:
                mkey = upload_date.strftime("%Y-%m")
                if mkey not in monthly_history:
                    monthly_history[mkey] = {"score": 0, "max": 0}
                monthly_history[mkey]["max"] += pts
                monthly_history[mkey]["score"] += pts

        elif task_type == "confirmation":
            latest = None
            latest_upload_date = None
            try:
                prefix = f"{hotel_id}/compliance/confirmations/{task_id}/"
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
                for obj in resp.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                        data = json.loads(meta["Body"].read().decode("utf-8"))
                        date_str = data.get("report_date") or data.get("confirmed_at")
                        if not date_str:
                            continue
                        report_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
                        if not latest or report_date > latest:
                            latest = report_date
                            latest_upload_date = obj["LastModified"]
            except Exception:
                pass

            if latest and is_still_valid(frequency, latest, now, grace_period):
                earned_points += points
                breakdown[task_id] = points
                # Track confirmation in monthly history using the upload date
                if latest_upload_date:
                    mkey = latest_upload_date.strftime("%Y-%m")
                    if mkey not in monthly_history:
                        monthly_history[mkey] = {"score": 0, "max": 0}
                    monthly_history[mkey]["score"] += points
                    monthly_history[mkey]["max"] += points
            else:
                breakdown[task_id] = 0

    result = {
        "score": earned_points,
        "max_score": total_points,
        "percent": round((earned_points / total_points) * 100, 1) if total_points else 0,
        "task_breakdown": breakdown,
        "monthly_history": dict(sorted(monthly_history.items()))
    }

    try:
        latest_key = f"{hotel_id}/compliance/latest.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=latest_key,
            Body=json.dumps(result, indent=2),
            ContentType="application/json"
        )
    except Exception as e:
        print(f"[WARN] Could not write latest.json for {hotel_id}: {e}")

    return result

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
