# --- FastAPI Audit Routes (audit.py) ---

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Body
from botocore.exceptions import ClientError
from datetime import datetime
import boto3
import json
import os

from app.routers.compliance_history import add_history_entry

router = APIRouter()

BUCKET = os.getenv("AWS_BUCKET_NAME")
APPROVAL_LOG_KEY = "logs/compliance-history/approval_log.json"
s3 = boto3.client("s3")


def update_approval_log(action: str, entry: dict):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=APPROVAL_LOG_KEY)
        body = obj["Body"].read().decode("utf-8")
        log = json.loads(body) if body.strip() else []
    except (ClientError, json.JSONDecodeError) as e:
        print(f"⚠️ Starting fresh approval log: {e}")
        log = []

    if action == "add":
        log.append(entry)
    elif action == "remove":
        log = [e for e in log if not (
            e.get("hotel_id") == entry.get("hotel_id") and
            e.get("task_id") == entry.get("task_id") and
            e.get("uploaded_at") == entry.get("uploaded_at")
        )]

    print("🔁 Writing to approval_log.json:", len(log), "entries")
    s3.put_object(
        Bucket=BUCKET,
        Key=APPROVAL_LOG_KEY,
        Body=json.dumps(log, indent=2),
        ContentType="application/json"
    )


@router.get("/compliance/history/approval-log")
def get_approval_log():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=APPROVAL_LOG_KEY)
        body = obj["Body"].read().decode("utf-8")
        log = json.loads(body) if body.strip() else []
        return {"entries": log}
    except (ClientError, json.JSONDecodeError):
        return {"entries": []}


@router.post("/compliance/history/approve")
def mark_approved(data: dict = Body(...)):
    hotel_id = data["hotel_id"]
    task_id = data["task_id"]
    timestamp = data["timestamp"]

    hotel_log_key = f"logs/compliance-history/{hotel_id}.json"

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=hotel_log_key)
        hotel_log = json.loads(obj["Body"].read())
    except ClientError:
        raise HTTPException(status_code=404, detail="Hotel history file not found")

    updated = False
    for item in hotel_log.get(task_id, []):
        if item.get("uploaded_at") == timestamp:
            item["approved"] = True
            updated = True

    if not updated:
        raise HTTPException(status_code=404, detail="Upload entry not found")

    s3.put_object(
        Bucket=BUCKET,
        Key=hotel_log_key,
        Body=json.dumps(hotel_log, indent=2),
        ContentType="application/json"
    )

    update_approval_log("remove", {
        "hotel_id": hotel_id,
        "task_id": task_id,
        "uploaded_at": timestamp
    })

    return {"success": True}


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

        # Save to hotel log
        add_history_entry(hotel_id, task_id, entry)

        # Save to approval log
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
