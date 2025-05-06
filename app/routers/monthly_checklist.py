from fastapi import APIRouter, HTTPException, Body
from datetime import datetime
from typing import List
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

CONFIRM_BUCKET = os.getenv("AWS_BUCKET_NAME")
CONFIRM_PREFIX = "confirmations"  # stored as: confirmations/{hotel_id}/{task_id}/...

@router.get("/api/compliance/monthly-checklist/{hotel_id}")
def get_monthly_checklist(hotel_id: str):
    DATA_PATH = "app/data/compliance.json"
    now = datetime.utcnow()
    year_month = now.strftime("%Y-%m")

    try:
        with open(DATA_PATH, "r") as f:
            raw_sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load task rules: {e}")

    tasks = []
    for section in raw_sections:
        for task in section["tasks"]:
            if task.get("type") == "confirmation" and task.get("needs_report") == "no":
                task_id = task["task_id"]
                key_prefix = f"{CONFIRM_PREFIX}/{hotel_id}/{task_id}/{year_month}"

                # look for confirmation file
                try:
                    resp = s3.list_objects_v2(Bucket=CONFIRM_BUCKET, Prefix=key_prefix)
                    confirmed = any(obj["Key"].endswith(".json") for obj in resp.get("Contents", []))
                    if confirmed:
                        metadata = s3.get_object(
                            Bucket=CONFIRM_BUCKET,
                            Key=f"{key_prefix}.json"
                        )
                        meta_data = json.loads(metadata["Body"].read().decode("utf-8"))
                        last_date = meta_data.get("confirmed_at")
                    else:
                        last_date = None
                except Exception:
                    last_date = None

                tasks.append({
                    "task_id": task_id,
                    "label": task["label"],
                    "frequency": task["frequency"],
                    "category": task["category"],
                    "points": task["points"],
                    "info_popup": task["info_popup"],
                    "last_confirmed_date": last_date,
                    "is_confirmed_this_month": last_date is not None
                })

    return tasks


@router.post("/api/compliance/confirm-task")
def confirm_task(
    data: dict = Body(...)
):
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    user_email = data.get("user_email", "system")  # replace with session auth later

    if not hotel_id or not task_id:
        raise HTTPException(status_code=400, detail="Missing hotel_id or task_id")

    now = datetime.utcnow()
    year_month = now.strftime("%Y-%m")
    s3_key = f"{CONFIRM_PREFIX}/{hotel_id}/{task_id}/{year_month}.json"

    confirmation_record = {
        "hotel_id": hotel_id,
        "task_id": task_id,
        "confirmed_by": user_email,
        "confirmed_at": now.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    try:
        s3.put_object(
            Body=json.dumps(confirmation_record, indent=2),
            Bucket=CONFIRM_BUCKET,
            Key=s3_key,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving confirmation: {str(e)}")

    return {"message": "Confirmation recorded", "confirmed_at": confirmation_record["confirmed_at"]}
