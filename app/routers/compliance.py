from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
import boto3

from app.schemas.common import SuccessResponse
from app.schemas.safety import SafetyScoreResponse, WeeklyScore
from app.utils.compliance_history import add_history_entry

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

# ---------- Upload Compliance File & Update Score + History ----------
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
        # Upload file and metadata to S3
        s3.upload_fileobj(file.file, BUCKET, s3_key)
        file_url = f"https://{BUCKET}.s3.amazonaws.com/{s3_key}"

        metadata = {
            "report_date": parsed_date.strftime("%Y-%m-%d"),
            "uploaded_at": datetime.utcnow().isoformat(),
            "filename": safe_filename,
            "fileUrl": file_url,
        }
        s3.put_object(Body=json.dumps(metadata, indent=2), Bucket=BUCKET, Key=metadata_key)

        # Log to compliance history
        entry = {
            "fileName": safe_filename,
            "reportDate": metadata["report_date"],
            "uploadedAt": metadata["uploaded_at"],
            "uploadedBy": "SYSTEM",
            "fileUrl": file_url,
            "type": "upload"
        }
        add_history_entry(hotel_id, task_id, entry)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading to S3: {str(e)}")

    # Recalculate compliance score
    from .compliance_score import get_compliance_score
    updated_score = get_compliance_score(hotel_id)

    # Save this month's snapshot
    now_month = datetime.utcnow().strftime("%Y-%m")
    try:
        if updated_score.get("monthly_history"):
            this_month = updated_score["monthly_history"].get(now_month)
            if this_month:
                month_key = f"{hotel_id}/compliance/monthly/{now_month}.json"
                s3.put_object(
                    Body=json.dumps(this_month, indent=2),
                    Bucket=BUCKET,
                    Key=month_key
                )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save monthly snapshot: {str(e)}")

    # Save latest.json snapshot
    try:
        latest_score = {
            "score": updated_score["score"],
            "max_score": updated_score["max_score"],
            "percent": updated_score["percent"],
            "timestamp": datetime.utcnow().isoformat(),
            "month": now_month,
            "task_breakdown": updated_score.get("task_breakdown", {})
        }
        latest_key = f"{hotel_id}/compliance/latest.json"
        s3.put_object(
            Body=json.dumps(latest_score, indent=2),
            Bucket=BUCKET,
            Key=latest_key
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save latest.json: {str(e)}")

    return {
        "id": s3_key,
        "message": "Compliance file uploaded and history saved",
        "score": updated_score
    }
