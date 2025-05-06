# /app/routers/due_tasks.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
import boto3

router = APIRouter()
load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

RULES_PATH = "app/data/compliance.json"
BUCKET = os.getenv("AWS_BUCKET_NAME")

# ---- POST endpoint to acknowledge task ----
class AcknowledgePayload(BaseModel):
    hotel_id: str
    task_id: str

@router.post("/api/compliance/acknowledge-task")
async def acknowledge_task(payload: AcknowledgePayload):
    now = datetime.utcnow()
    key = f"{payload.hotel_id}/acknowledged/{payload.task_id}-{now.strftime('%Y-%m')}.json"
    body = json.dumps({"acknowledged_at": now.isoformat()})

    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        return {"message": "Task acknowledged successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save acknowledgment: {e}")

# ---- GET endpoint to fetch due and next month tasks ----
@router.get("/api/compliance/due-tasks/{hotel_id}")
async def get_due_tasks(hotel_id: str):
    now = datetime.utcnow()
    month_start = now.replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)

    try:
        with open(RULES_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    due_this_month = []
    next_month_due = []

    for section in sections:
        for task in section["tasks"]:
            if task["type"] != "upload":
                continue

            freq = task["frequency"].lower()
            period = {
                "monthly": 30,
                "quarterly": 90,
                "twice annually": 180,
                "annually": 365,
                "biennially": 730,
                "every 5 years": 1825
            }.get(freq, 0)

            if not period:
                continue

            task_id = task["task_id"]
            prefix = f"{hotel_id}/compliance/{task_id}/"
            latest = None

            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
                for obj in resp.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        metadata = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                        data = json.loads(metadata["Body"].read().decode("utf-8"))
                        report_date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                        if not latest or report_date > latest:
                            latest = report_date
            except Exception:
                pass

            next_due = (latest + timedelta(days=period)) if latest else datetime.min
            
            # Check if acknowledged for next month
            ack_key = f"{hotel_id}/acknowledged/{task_id}-{next_month.strftime('%Y-%m')}.json"
            is_acknowledged = False
            try:
                s3.head_object(Bucket=BUCKET, Key=ack_key)
                is_acknowledged = True
            except:
                pass

            if month_start <= next_due < next_month:
                due_this_month.append(task)
            elif next_month <= next_due < (next_month + timedelta(days=31)) and not is_acknowledged:
                next_month_due.append(task)

    return {
        "due_this_month": due_this_month,
        "next_month_uploadables": next_month_due
    }
