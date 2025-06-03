from fastapi import APIRouter, HTTPException
import json
import os

router = APIRouter()

@router.get("/tasks/{hotel_id}")
def get_all_tasks(hotel_id: str):
    try:
        from app.s3_config import get_hotel_compliance_tasks
        all_tasks = get_hotel_compliance_tasks(hotel_id)
        
        # Add hotel_id to each task and return
        tasks_with_hotel_id = []
        for task in all_tasks:
            tasks_with_hotel_id.append({
                **task,
                "hotel_id": hotel_id
            })
        
        return {"tasks": tasks_with_hotel_id}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance tasks: {e}")

@router.get("/task-labels")
def get_task_labels():
    try:
        with open("app/data/compliance.json", "r") as f:
            sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load task labels: {e}")
    
    label_map = {}
    for section in sections:
        for task in section.get("tasks", []):
            label_map[task["task_id"]] = task.get("label", task["task_id"])
    return label_map
