from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
import os
import json
import boto3

router = APIRouter()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

@router.get("/due-tasks/{hotel_id}")
async def get_due_tasks(hotel_id: str):
    now = datetime.utcnow()
    month_start = now.replace(day=1)
    next_month = (month_start + timedelta(days=32)).replace(day=1)
    next_month_end = (next_month + timedelta(days=32)).replace(day=1)

    try:
        from app.s3_config import get_hotel_compliance_tasks
        all_tasks = get_hotel_compliance_tasks(hotel_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    due_this_month = []
    next_month_due = []

    for task in all_tasks:
        task_id = task["task_id"]
        frequency = task.get("frequency", "").lower()
        task_type = task.get("type", "upload")

        if task_type != "upload":
            continue

        interval = {
            "monthly": 30,
            "quarterly": 90,
            "twice annually": 180,
            "annually": 365,
            "biennially": 730,
            "every 5 years": 1825
        }.get(frequency, 0)

        if interval == 0:
            continue

        latest = None
        prefix = f"{hotel_id}/compliance/{task_id}/"
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

        # ✅ If never uploaded, consider it due immediately
        if latest is None:
            due_this_month.append(task)
            continue

        next_due = latest + timedelta(days=interval)

        ack_key = f"{hotel_id}/acknowledged/{task_id}-{next_month.strftime('%Y-%m')}.json"
        is_acknowledged = False
        try:
            s3.head_object(Bucket=BUCKET, Key=ack_key)
            is_acknowledged = True
        except:
            pass

        if month_start <= next_due < next_month:
            due_this_month.append(task)
        elif next_month <= next_due < next_month_end and not is_acknowledged:
            next_month_due.append(task)

    return {
        "due_this_month": due_this_month,
        "next_month_uploadables": next_month_due
    }
