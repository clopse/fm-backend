from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

# Adjust path to be absolute if needed
RULES_PATH = "app/data/compliance.json"

@router.get("/tasks/{hotel_id}")
def get_all_tasks(hotel_id: str):
    try:
        from app.s3_config import get_hotel_compliance_tasks
        # ... then in the function:
        all_tasks = get_hotel_compliance_tasks(hotel_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance tasks: {e}")

    all_tasks = []
    for section in sections:
        for task in section.get("tasks", []):
            all_tasks.append({
                **task,
                "hotel_id": hotel_id
            })

    return {"tasks": all_tasks}


@router.get("/task-labels")
def get_task_labels():
    try:
        with open(RULES_PATH, "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load task labels: {e}")

    label_map = {}
    for section in sections:
        for task in section.get("tasks", []):
            label_map[task["task_id"]] = task.get("label", task["task_id"])

    return label_map
