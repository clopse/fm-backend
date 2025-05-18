from fastapi import APIRouter, HTTPException, Request
import json
import boto3
from datetime import datetime

router = APIRouter()

s3 = boto3.client("s3")
BUCKET_NAME = "jmk-project-uploads"
APPROVAL_LOG_KEY = "logs/compliance-history/approval_log.json"


def _get_history_key(hotel_id: str) -> str:
    return f"logs/compliance-history/{hotel_id}.json"


def load_compliance_history(hotel_id: str) -> dict:
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=_get_history_key(hotel_id))
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load history: {e}")


def save_compliance_history(hotel_id: str, history: dict):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=_get_history_key(hotel_id),
        Body=json.dumps(history, indent=2),
        ContentType="application/json"
    )


def update_approval_log(action: str, entry: dict):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        log = json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        log = []

    if action == "remove":
        log = [e for e in log if not (
            e.get("hotel_id") == entry.get("hotel_id") and
            e.get("task_id") == entry.get("task_id") and
            e.get("uploaded_at") == entry.get("uploaded_at")
        )]

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=APPROVAL_LOG_KEY,
        Body=json.dumps(log, indent=2),
        ContentType="application/json"
    )


@router.post("/history/approve")
async def approve_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")

    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    found = False

    for entry in entries:
        if entry.get("uploaded_at") == timestamp:
            entry["approved"] = True
            found = True
            break

    if not found:
        raise HTTPException(status_code=404, detail="Entry not found")

    save_compliance_history(hotel_id, history)
    update_approval_log("remove", {"hotel_id": hotel_id, "task_id": task_id, "uploaded_at": timestamp})
    return {"success": True, "message": "Approved"}


@router.post("/history/reject")
async def reject_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")
    reason = data.get("reason", "No reason given")

    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    filtered = [e for e in entries if e.get("uploaded_at") != timestamp]

    if len(filtered) == len(entries):
        raise HTTPException(status_code=404, detail="Entry not found")

    history[task_id] = filtered
    save_compliance_history(hotel_id, history)
    update_approval_log("remove", {"hotel_id": hotel_id, "task_id": task_id, "uploaded_at": timestamp})

    return {"success": True, "message": f"Rejected: {reason}"}


@router.delete("/history/delete")
async def delete_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")

    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    filtered = [e for e in entries if e.get("uploaded_at") != timestamp]

    if len(filtered) == len(entries):
        raise HTTPException(status_code=404, detail="Entry not found")

    history[task_id] = filtered
    save_compliance_history(hotel_id, history)
    update_approval_log("remove", {"hotel_id": hotel_id, "task_id": task_id, "uploaded_at": timestamp})

    return {"success": True, "message": "Deleted"}
