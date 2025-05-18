# --- app/routers/compliance_history.py ---

from fastapi import APIRouter, HTTPException
import json
import boto3
from datetime import datetime

router = APIRouter()

s3 = boto3.client("s3")
BUCKET_NAME = "jmk-project-uploads"
APPROVAL_LOG_KEY = "logs/compliance-history/approval_log.json"

# ✅ NEW PATH STRUCTURE
def _get_history_key(hotel_id: str) -> str:
    return f"logs/compliance-history/{hotel_id}.json"

def load_compliance_history(hotel_id: str) -> dict:
    key = _get_history_key(hotel_id)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        print(f"📥 Loaded history for {hotel_id}")
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        print(f"⚠️ No history file found for {hotel_id}, starting fresh.")
        return {}
    except Exception as e:
        print(f"[ERROR] Failed to load compliance history for {hotel_id}: {e}")
        return {}

def save_compliance_history(hotel_id: str, history: dict):
    key = _get_history_key(hotel_id)
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(history, indent=2),
            ContentType="application/json"
        )
        print(f"✅ Saved history for {hotel_id} to {key}")
    except Exception as e:
        print(f"[ERROR] Failed to save compliance history for {hotel_id}: {e}")

def update_approval_log(action: str, entry: dict):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        body = obj["Body"].read().decode("utf-8")
        log = json.loads(body) if body.strip() else []
        print(f"📄 Loaded existing approval log with {len(log)} entries")
    except (s3.exceptions.NoSuchKey, json.JSONDecodeError) as e:
        print(f"⚠️ approval_log.json missing or invalid: {e}, starting fresh.")
        log = []

    if action == "add":
        log.append(entry)
        print(f"➕ Adding new entry to approval log: {entry}")
    elif action == "remove":
        before = len(log)
        log = [e for e in log if not (
            e.get("hotel_id") == entry.get("hotel_id") and
            e.get("task_id") == entry.get("task_id") and
            e.get("uploaded_at") == entry.get("uploaded_at")
        )]
        print(f"➖ Removed entry from approval log (before: {before}, after: {len(log)})")

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=APPROVAL_LOG_KEY,
            Body=json.dumps(log, indent=2),
            ContentType="application/json"
        )
        print(f"✅ Wrote {len(log)} entries to approval_log.json")
    except Exception as e:
        print(f"[ERROR] Failed to update approval log: {e}")

def add_history_entry(hotel_id: str, task_id: str, entry: dict):
    print(f"➡️ add_history_entry called for hotel {hotel_id}, task {task_id}")
    history = load_compliance_history(hotel_id)
    if task_id not in history:
        history[task_id] = []
    history[task_id].insert(0, entry)
    history[task_id] = history[task_id][:50]
    save_compliance_history(hotel_id, history)

    if entry.get("type") == "upload" and not entry.get("approved"):
        update_approval_log("add", {
            "hotel_id": hotel_id,
            "task_id": task_id,
            "report_date": entry.get("report_date"),
            "uploaded_at": entry.get("uploaded_at"),
            "filename": entry.get("filename"),
            "fileUrl": entry.get("fileUrl"),
            "uploaded_by": entry.get("uploaded_by"),
            "type": "upload"
        })

def delete_history_entry(hotel_id: str, task_id: str, timestamp: str):
    history = load_compliance_history(hotel_id)
    if task_id in history:
        history[task_id] = [
            e for e in history[task_id]
            if e.get("uploadedAt") != timestamp and e.get("confirmedAt") != timestamp
        ]
        save_compliance_history(hotel_id, history)

@router.get("/history/{hotel_id}")
async def get_compliance_history(hotel_id: str):
    try:
        history = load_compliance_history(hotel_id)
        return {"hotel_id": hotel_id, "history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance history: {e}")
