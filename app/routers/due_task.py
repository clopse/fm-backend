from fastapi import APIRouter, HTTPException
from datetime import datetime
import os
import json

router = APIRouter()

@router.get("/api/compliance/due-tasks/{hotel_id}")
def get_due_tasks(hotel_id: str):
    DATA_PATH = "app/data/compliance.json"
    now = datetime.utcnow()
    month = now.month

    try:
        with open(DATA_PATH, "r") as f:
            raw_sections = json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    due_now = []
    due_soon = []

    for section in raw_sections:
        for task in section["tasks"]:
            if task["type"] != "upload":
                continue

            task_id = task["task_id"]
            frequency = task.get("frequency", "Annually")
            label = task["label"]

            # Determine if it's expected this month or next
            if is_due_this_month(frequency, month):
                due_now.append({
                    "task_id": task_id,
                    "label": label,
                    "category": task.get("category"),
                    "frequency": frequency,
                    "info_popup": task.get("info_popup"),
                    "due": "this_month"
                })
            elif is_due_this_month(frequency, month + 1):
                due_soon.append({
                    "task_id": task_id,
                    "label": label,
                    "category": task.get("category"),
                    "frequency": frequency,
                    "info_popup": task.get("info_popup"),
                    "due": "next_month"
                })

    return {
        "due_now": due_now,
        "due_soon": due_soon
    }

def is_due_this_month(frequency: str, month: int) -> bool:
    if month > 12:
        month = 1
    if frequency == "Monthly":
        return True
    if frequency == "Quarterly":
        return month in [3, 6, 9, 12]
    if frequency == "Twice Annually":
        return month in [6, 12]
    if frequency == "Annually":
        return month == 12
    if frequency == "Biennially":
        return month == 12
    if frequency == "Every 5 Years":
        return month == 12
    return False
