from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import boto3

from app.utils.compliance_history import add_history_entry

load_dotenv()

router = APIRouter()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

# ---------------- Upload File ----------------
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
        s3.upload_fileobj(file.file, BUCKET, s3_key)
        file_url = f"https://{BUCKET}.s3.amazonaws.com/{s3_key}"

        metadata = {
            "report_date": parsed_date.strftime("%Y-%m-%d"),
            "uploaded_at": datetime.utcnow().isoformat(),
            "filename": safe_filename,
            "fileUrl": file_url,
        }
        s3.put_object(Body=json.dumps(metadata, indent=2), Bucket=BUCKET, Key=metadata_key)

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

    return {
        "id": s3_key,
        "message": "Compliance file uploaded and logged",
        "fileUrl": file_url
    }


# ---------------- Confirm Task (No File) ----------------
@router.post("/confirm-task", response_model=dict)
async def confirm_task(
    hotel_id: str = Form(...),
    task_id: str = Form(...),
    user_email: str = Form(...)
):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    timestamp = datetime.utcnow().isoformat()
    confirm_key = f"{hotel_id}/confirmations/{task_id}/{today}.json"

    metadata = {
        "report_date": today,
        "confirmed_by": user_email,
        "confirmed_at": timestamp,
        "type": "confirmation"
    }

    try:
        s3.put_object(Bucket=BUCKET, Key=confirm_key, Body=json.dumps(metadata, indent=2))

        entry = {
            "reportDate": today,
            "uploadedAt": timestamp,
            "uploadedBy": user_email,
            "fileUrl": None,
            "type": "confirmation"
        }
        add_history_entry(hotel_id, task_id, entry)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to confirm task: {str(e)}")

    return {
        "message": "Task confirmed",
        "confirmed_at": timestamp
    }
