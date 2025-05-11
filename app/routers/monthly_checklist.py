from fastapi import APIRouter, HTTPException, Body
from datetime import datetime
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
RULES_PATH = "app/data/compliance.json"


@router.get("/monthly-checklist/{hotel_id}")
def get_monthly_checklist(hotel_id: str):
    now = datetime.utcnow()
    year_month = now.strftime("%Y-%m")

    try:
        with open(RULES_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    results = []
    for section in sections:
        for task in section["tasks"]:
            if task.get("type") == "confirmation" and task.get("needs_report") == "no":
                task_id = task["task_id"]
                key = f"{hotel_id}/compliance/confirmations/{task_id}/{year_month}.json"

                is_confirmed = False
                last_date = None

                try:
                    metadata = s3.get_object(Bucket=BUCKET, Key=key)
                    data = json.loads(metadata["Body"].read().decode("utf-8"))
                    last_date = data.get("confirmed_at")
                    is_confirmed = last_date is not None
                except Exception:
                    pass

                if not is_confirmed:
                    results.append({
                        "task_id": task_id,
                        "label": task.get("label", ""),
                        "frequency": task.get("frequency", ""),
                        "category": task.get("category", ""),
                        "points": task.get("points", 0),
                        "info_popup": task.get("info_popup", ""),
                        "last_confirmed_date": last_date,
                        "is_confirmed_this_month": is_confirmed
                    })

    return results


@router.post("/confirm-task")
def confirm_task(data: dict = Body(...)):
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    user_email = data.get("user_email", "system")

    if not hotel_id or not task_id:
        raise HTTPException(status_code=400, detail="Missing hotel_id or task_id")

    now = datetime.utcnow()
    year_month = now.strftime("%Y-%m")
    key = f"{hotel_id}/compliance/confirmations/{task_id}/{year_month}.json"

    record = {
        "hotel_id": hotel_id,
        "task_id": task_id,
        "confirmed_by": user_email,
        "confirmed_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "report_date": now.strftime("%Y-%m-%d")
    }

    try:
        s3.put_object(Body=json.dumps(record, indent=2), Bucket=BUCKET, Key=key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save confirmation: {str(e)}")

    return {"message": "Confirmation recorded", "confirmed_at": record["confirmed_at"]}
