# /app/routers/due_tasks.py
from fastapi import APIRouter, HTTPException
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

@router.get("/api/compliance/due-tasks/{hotel_id}")
async def get_due_tasks(hotel_id: str):
    now = datetime.utcnow()
    month_start = now.replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    three_months_ago = now - timedelta(days=90)

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
            grace_days = 30
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
            if month_start <= next_due < next_month:
                due_this_month.append(task)
            elif next_month <= next_due < (next_month + timedelta(days=31)):
                next_month_due.append(task)

    return {
        "due_this_month": due_this_month,
        "next_month_uploadables": next_month_due
    }
