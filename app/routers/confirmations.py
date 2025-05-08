from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
import boto3
import os
import json

from dotenv import load_dotenv
from app.utils.compliance_history import add_history_entry

load_dotenv()

router = APIRouter()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

BUCKET = os.getenv("AWS_BUCKET_NAME")


class ConfirmPayload(BaseModel):
    hotel_id: str
    task_id: str
    user_email: str


@router.post("/confirm-task")
async def confirm_task(payload: ConfirmPayload):
    now = datetime.utcnow()
    history_entry = {
        "confirmedAt": now.isoformat(),
        "user": payload.user_email,
        "type": "confirmation",
        "approved": False
    }

    try:
        add_history_entry(payload.hotel_id, payload.task_id, history_entry)
        return {"message": "Task confirmed and logged."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to log confirmation: {str(e)}")
