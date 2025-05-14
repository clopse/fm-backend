from fastapi import APIRouter, HTTPException, UploadFile, File, Form
import boto3
import os
import json
from botocore.exceptions import ClientError
from datetime import datetime

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


@router.get("/history/all")
def get_audit_queue():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=APPROVAL_LOG_KEY)
        entries = json.loads(obj["Body"].read())
        return {"entries": entries}
    except Exception as e:
        print(f"Error reading approval log: {e}")
        return {"entries": []}


@router.post("/history/approve")
def approve_compliance_entry(data: dict):
    try:
        hotel_id = data.get("hotel_id")
        task_id = data.get("task_id")
        timestamp = data.get("timestamp")

        if not all([hotel_id, task_id, timestamp]):
            raise HTTPException(status_code=400, detail="Missing required fields")

        key = f"logs/compliance-history/{hotel_id}.json"
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            history_data = json.loads(obj["Body"].read())
        except Exception:
            raise HTTPException(status_code=404, detail=f"Could not find history for hotel: {hotel_id}")

        updated = False
        for entry in history_data.get(task_id, []):
            if entry.get("uploaded_at") == timestamp:
                entry["approved"] = True
                updated = True

        if not updated:
            raise HTTPException(status_code=404, detail="Entry not found")

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(history_data, indent=2),
            ContentType="application/json"
        )

        update_approval_log("remove", {
            "hotel_id": hotel_id,
            "task_id": task_id,
            "uploaded_at": timestamp
        })

        return {"success": True, "message": "Entry approved"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error approving entry: {e}")
        raise HTTPException(status_code=500, detail="Approval failed")


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

        # Save to hotel history
        history_key = f"logs/compliance-history/{hotel_id}.json"
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=history_key)
            history_data = json.loads(obj["Body"].read())
        except ClientError:
            history_data = {}

        history_data.setdefault(task_id, []).append(entry)

        s3.put_object(
            Bucket=BUCKET,
            Key=history_key,
            Body=json.dumps(history_data, indent=2),
            ContentType="application/json"
        )

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
