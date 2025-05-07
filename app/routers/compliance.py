from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
import boto3

from app.schemas.common import SuccessResponse
from app.schemas.safety import SafetyScoreResponse, WeeklyScore

load_dotenv()

# AWS S3 setup
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")
DATA_PATH = "app/data/compliance.json"

router = APIRouter()

# ---------- Upload Compliance File & Update Score ----------
@router.post("/uploads/compliance", response_model=dict)
async def upload_compliance_doc(
    hotel_id: str = Form(...),
    task_id: str = Form(...),
    report_date: str = Form(...),
    file: UploadFile = File(...)
):
    if not file or not file.file:
        raise HTTPException(status_code=400, detail="No file received")

    try:
        parsed_date = datetime.strptime(report_date, "%Y-%m-%d")
        if parsed_date > datetime.utcnow():
            raise HTTPException(status_code=400, detail="Report date cannot be in the future.")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid report_date format. Use YYYY-MM-DD.")

    report_tag = parsed_date.strftime('%Y%m%d')
    unique_suffix = datetime.utcnow().strftime('%H%M%S')
    safe_filename = file.filename.replace(" ", "_")
    s3_key = f"{hotel_id}/compliance/{task_id}/{report_tag}_{unique_suffix}_{safe_filename}"
    metadata_key = s3_key + ".json"

    try:
        # Upload file and metadata
        s3.upload_fileobj(file.file, BUCKET, s3_key)
        metadata = {
            "report_date": parsed_date.strftime("%Y-%m-%d"),
            "uploaded_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "filename": safe_filename,
        }
        s3.put_object(Body=json.dumps(metadata, indent=2), Bucket=BUCKET, Key=metadata_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading to S3: {str(e)}")

    # Recalculate compliance score
    from .compliance_score import get_compliance_score
    updated_score = get_compliance_score(hotel_id)

    # Save this month's score snapshot
    now_month = datetime.utcnow().strftime("%Y-%m")
    if updated_score.get("monthly_history"):
        this_month = updated_score["monthly_history"].get(now_month)
        if this_month:
            try:
                s3.put_object(
                    Body=json.dumps(this_month, indent=2),
                    Bucket=BUCKET,
                    Key=f"{hotel_id}/compliance/monthly/{now_month}.json"
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to save monthly score: {str(e)}")

    return {
        "id": s3_key,
        "message": "Compliance file uploaded successfully",
        "score": updated_score
    }

# ---------- Get Current Compliance Score ----------
@router.get("/api/compliance/score/{hotel_id}", response_model=SafetyScoreResponse)
def get_compliance_score(hotel_id: str):
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

    for section in sections:
        for task in section["tasks"]:
            if task["type"] != "upload":
                continue

            task_id = task["task_id"]
            points = task.get("points", 20)
            frequency = task.get("frequency", "Annually")

            total_points += points
            valid_files = []

            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{reports_base}/{task_id}/")
                for obj in resp.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                        data = json.loads(meta["Body"].read().decode("utf-8"))
                        report_date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                        if is_still_valid(frequency, report_date, now, grace_period):
                            valid_files.append(report_date)
            except Exception:
                pass

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

    return SafetyScoreResponse(
        total_points=total_points,
        earned_points=earned_points,
        score_percent=round((earned_points / total_points) * 100) if total_points else 0,
        breakdown=breakdown
    )

# ---------- Weekly Score History ----------
@router.get("/api/compliance/score-history/{hotel_id}", response_model=list[WeeklyScore])
def get_compliance_score_history(hotel_id: str):
    reports_base = f"{hotel_id}/compliance"

    try:
        with open(DATA_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    history = {week: {"earned": 0, "total": 0} for week in range(1, 54)}

    for section in sections:
        for task in section["tasks"]:
            if task["type"] != "upload":
                continue

            task_id = task["task_id"]
            points = task.get("points", 20)

            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{reports_base}/{task_id}/")
                for obj in resp.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                        data = json.loads(meta["Body"].read().decode("utf-8"))
                        report_date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                        week = int(report_date.strftime("%W")) + 1
                        history[week]["earned"] += points
                        history[week]["total"] += points
            except Exception:
                continue

    return [
        WeeklyScore(week=week, score=round((data["earned"] / data["total"]) * 100, 1))
        for week, data in history.items() if data["total"] > 0
    ]

# ---------- Helper Functions ----------
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
