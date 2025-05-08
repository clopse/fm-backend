from fastapi import APIRouter, HTTPException
import json

router = APIRouter()

RULES_PATH = "app/data/compliance.json"

@router.get("/tasks/{hotel_id}")
def get_all_tasks(hotel_id: str):
    try:
        with open(RULES_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance tasks: {e}")
    
    # Flatten tasks and include hotel_id for clarity
    all_tasks = []
    for section in sections:
        for task in section.get("tasks", []):
            all_tasks.append({
                **task,
                "hotel_id": hotel_id
            })
    
    return {"tasks": all_tasks}
