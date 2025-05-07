from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import datetime
from utils.compliance_history import add_history_entry

router = APIRouter()

class ConfirmationInput(BaseModel):
    hotel_id: str
    task_id: str
    user: str  # e.g. “Dave”, or later from auth

@router.post("/confirmations/")
async def confirm_task(data: ConfirmationInput):
    try:
        entry = {
            "confirmedAt": datetime.utcnow().isoformat(),
            "confirmedBy": data.user,
            "type": "confirmation"
        }
        add_history_entry(data.hotel_id, data.task_id, entry)
        return {"message": "Task confirmed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
