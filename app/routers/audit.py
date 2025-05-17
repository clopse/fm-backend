from fastapi import APIRouter, HTTPException, UploadFile, File, Form
import boto3
import os
import json
from datetime import datetime
from botocore.exceptions import ClientError
from app.routers.compliance_history import add_history_entry

router = APIRouter()
BUCKET = os.getenv("AWS_BUCKET_NAME")
s3 = boto3.client("s3")

APPROVAL_LOG_KEY = "logs/compliance-history/approval_log.json"

def update_approval_log(action: str, entry: dict):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=APPROVAL_LOG_KEY)
        log = json.loads(obj["Body"].read())
    except ClientError:
        log = []

    if action == "add":
        log.append(entry)
    elif action == "remove":
        log = [e for e in log if not (
            e.get("hotel_id") == entry.get("hotel_id") and
            e.get("task_id") == entry.get("task_id") and
            e.get("uploaded_at") == entry.get("uploaded_at")
        )]

    s3.put_object(
        Bucket=BUCKET,
        Key=APPROVAL_LOG_KEY,
        Body=json.dumps(log, indent=2),
        ContentType="application/json"
    )

@router.post("/uploads/compliance")
async def upload_compliance_doc(
    hotel_id: str = Form(...),
    task_id: str = Form(...),
    report_date: str = Form(...),
    file: UploadFile = File(...),
    uploaded_by: str = Form("SYSTEM")
):
    try:
        now = datetime.utcnow()
        now_str = now.isoformat()
        filename = file.filename
        key = f"{hotel_id}/compliance/{task_id}/{report_date.replace('-', '')}_{now_str.replace(':', '').replace('.', '')}_{filename}"

        file_content = await file.read()
        s3.put_object(Bucket=BUCKET, Key=key, Body=file_content)

        file_url = f"https://{BUCKET}.s3.amazonaws.com/{key}"
        entry = {
            "report_date": report_date,
            "uploaded_at": now_str,
            "filename": filename,
            "fileUrl": file_url,
            "uploaded_by": uploaded_by,
            "type": "upload",
            "approved": False,
            "loggedAt": now_str
        }

        # ✅ Add entry via shared handler (uses full history retention logic)
        add_history_entry(hotel_id, task_id, entry)

        # ✅ Log to approval log
        update_approval_log("add", {
            "hotel_id": hotel_id,
            "task_id": task_id,
            "report_date": report_date,
            "uploaded_at": now_str,
            "filename": filename,
            "fileUrl": file_url,
            "uploaded_by": uploaded_by,
            "type": "upload"
        })

        return {"success": True, "message": "Upload successful"}

    except Exception as e:
        print(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail="Upload failed")
