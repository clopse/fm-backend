from fastapi import APIRouter, HTTPException
from app.utils.compliance_history import load_compliance_history

router = APIRouter()

@router.get("/compliance/history/{hotel_id}")
def get_compliance_history(hotel_id: str):
    """
    Returns all compliance history entries for all tasks in a hotel.
    """
    try:
        history = load_compliance_history(hotel_id)
        return {
            "hotel_id": hotel_id,
            "history": history
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance history: {e}")
