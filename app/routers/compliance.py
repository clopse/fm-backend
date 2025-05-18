from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import boto3
from app.routers.compliance_history import add_history_entry
from .compliance_score import get_compliance_score

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

router = APIRouter()


@router.post("/uploads/compliance")
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

    timestamp = datetime.utcnow()
    report_tag = parsed_date.strftime('%Y%m%d')
    unique_suffix = timestamp.strftime('%H%M%S')
    safe_filename = file.filename.replace(" ", "_")
    s3_key = f"{hotel_id}/compliance/{task_id}/{report_tag}_{unique_suffix}_{safe_filename}"
    metadata_key = s3_key + ".json"

    file_url = f"https://{BUCKET}.s3.amazonaws.com/{s3_key}"

    metadata = {
        "report_date": parsed_date.strftime("%Y-%m-%d"),
        "uploaded_at": timestamp.isoformat(),
        "filename": safe_filename,
        "fileUrl": file_url,
        "uploaded_by": "SYSTEM",
        "approved": False,
        "type": "upload"
    }

    try:
        s3.upload_fileobj(
            Fileobj=file.file,
            Bucket=BUCKET,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/pdf",
                "ContentDisposition": "inline"
            }
        )
        s3.put_object(Bucket=BUCKET, Key=metadata_key, Body=json.dumps(metadata, indent=2))
        add_history_entry(hotel_id, task_id, metadata)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload error: {str(e)}")

    return {"message": "Upload successful", "file": file_url}


@router.get("/compliance/task-labels")
def get_task_labels():
    try:
        with open("app/data/compliance.json") as f:
            data = json.load(f)
        label_map = {
            task["task_id"]: task["label"]
            for section in data
            for task in section.get("tasks", [])
        }
        return label_map
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load task labels: {e}")
