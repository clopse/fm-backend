from fastapi import APIRouter, HTTPException, Request
import json
import boto3
from datetime import datetime

router = APIRouter()

s3 = boto3.client("s3")
BUCKET_NAME = "jmk-project-uploads"
APPROVAL_LOG_KEY = "logs/compliance-history/approval_log.json"
RULES_PATH = "app/data/compliance.json"

# ✅ NEW PATH STRUCTURE
def _get_history_key(hotel_id: str) -> str:
    return f"logs/compliance-history/{hotel_id}.json"

def load_compliance_history(hotel_id: str) -> dict:
    key = _get_history_key(hotel_id)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception:
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
    except Exception:
        pass

def update_approval_log(action: str, entry: dict):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        body = obj["Body"].read().decode("utf-8")
        log = json.loads(body) if body.strip() else []
    except (s3.exceptions.NoSuchKey, json.JSONDecodeError):
        log = []

    if action == "add":
        log = [e for e in log if not (
            e.get("hotel_id") == entry.get("hotel_id") and
            e.get("task_id") == entry.get("task_id") and
            e.get("report_date") == entry.get("report_date")
        )]
        log.insert(0, entry)
    elif action == "remove":
        log = [e for e in log if not (
            e.get("hotel_id") == entry.get("hotel_id") and
            e.get("task_id") == entry.get("task_id") and
            e.get("uploaded_at") == entry.get("uploaded_at")
        )]

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=APPROVAL_LOG_KEY,
            Body=json.dumps(log, indent=2),
            ContentType="application/json"
        )
    except Exception:
        pass

def add_history_entry(hotel_id: str, task_id: str, entry: dict):
    history = load_compliance_history(hotel_id)
    if task_id not in history:
        history[task_id] = []

    history[task_id] = [
        e for e in history[task_id]
        if not (
            e.get("report_date") == entry.get("report_date") or
            e.get("filename") == entry.get("filename")
        )
    ]

    history[task_id].insert(0, entry)
    history[task_id] = history[task_id][:50]
    save_compliance_history(hotel_id, history)

    if entry.get("type") == "upload" and entry.get("approved") is not True:
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

@router.get("/history/approval-log")
async def get_approval_log():
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        body = obj["Body"].read().decode("utf-8")
        log = json.loads(body) if body.strip() else []
        return {"entries": log}
    except s3.exceptions.NoSuchKey:
        return {"entries": []}
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to load approval log")

@router.get("/history/{hotel_id}")
async def get_compliance_history(hotel_id: str):
    try:
        history = load_compliance_history(hotel_id)
        return {"hotel_id": hotel_id, "history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance history: {e}")

@router.post("/history/approve")
async def approve_compliance_entry(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")

    if not hotel_id or not task_id or not timestamp:
        raise HTTPException(status_code=400, detail="Missing required fields.")

    update_approval_log("remove", {
        "hotel_id": hotel_id,
        "task_id": task_id,
        "uploaded_at": timestamp
    })

    history = load_compliance_history(hotel_id)
    if task_id in history:
        for entry in history[task_id]:
            if entry.get("uploaded_at") == timestamp or entry.get("uploadedAt") == timestamp:
                entry["approved"] = True
                break
        save_compliance_history(hotel_id, history)

    return {"success": True}

@router.get("/history/matrix")
def get_compliance_matrix():
    try:
        # Keep using master compliance.json for matrix - we want to see ALL possible tasks
        with open(RULES_PATH, "r") as f:
            rules = json.load(f)
    except Exception:
        raise HTTPException(status_code=500, detail="Could not load compliance rules.")

    hotels = [
        "hiex", "hida", "hbhdcc", "hbhe", "sera", "moxy"
    ]
    all_tasks = [task["task_id"] for section in rules for task in section.get("tasks", [])]

    entries = []
    for hotel_id in hotels:
        history = load_compliance_history(hotel_id)
        for task_id in all_tasks:
            task_entries = history.get(task_id, [])
            if any(e.get("approved") for e in task_entries):
                status = "done"
            elif task_entries:
                status = "pending"
            else:
                status = "missing"
            entries.append({"hotel_id": hotel_id, "task_id": task_id, "status": status})

    return {"entries": entries}
