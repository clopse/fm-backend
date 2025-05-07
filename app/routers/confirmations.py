from fastapi import APIRouter
from pydantic import BaseModel
from utils.compliance_history import add_history_entry
from datetime import datetime

router = APIRouter()

class ConfirmationInput(BaseModel):
    hotel_id: str
    task_id: str
    user: str

@router.post("/confirmations/")
async def confirm_task(data: ConfirmationInput):
    entry = {
        "confirmedAt": datetime.utcnow().isoformat(),
        "confirmedBy": data.user,
        "type": "confirmation"
    }
    add_history_entry(data.hotel_id, data.task_id, entry)
    return {"message": "Task confirmed"}
