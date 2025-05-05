import os
import uuid
import json
import shutil
from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional

from app.parsers.arden import parse_arden  # or your docupanda-compatible parser

router = APIRouter()

STORAGE_ROOT = "storage"

def save_job_status(job_id: str, status: str, data: Optional[dict] = None):
    job_folder = os.path.join(STORAGE_ROOT, "jobs", job_id)
    os.makedirs(job_folder, exist_ok=True)
    status_path = os.path.join(job_folder, "status.json")

    with open(status_path, "w") as f:
        json.dump({
            "status": status,
            "data": data
        }, f)

@router.post("/utilities/parse-and-save")
async def parse_and_save_utility(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    hotel_id: str = Form(...),
    supplier: str = Form(...),
    utility_type: str = Form(...)
):
    # Generate job ID and create job folder
    job_id = str(uuid.uuid4())
    job_folder = os.path.join(STORAGE_ROOT, "jobs", job_id)
    os.makedirs(job_folder, exist_ok=True)

    # Save uploaded file
    file_path = os.path.join(job_folder, file.filename)
    with open(file_path, "wb") as f_out:
        shutil.copyfileobj(file.file, f_out)

    # Mark job as processing
    save_job_status(job_id, "processing")

    # Start background task
    background_tasks.add_task(process_file, job_id, file_path, hotel_id, supplier, utility_type)

    return JSONResponse(content={"jobId": job_id}, status_code=200)

def process_file(job_id: str, file_path: str, hotel_id: str, supplier: str, utility_type: str):
    try:
        # Simulate parsing logic (replace with real parser)
        if supplier.lower() == "docupanda":
            parsed_data = parse_arden(file_path)  # your custom parser
        else:
            parsed_data = {"error": "Unknown supplier"}  # fallback

        # Save completed status
        save_job_status(job_id, "completed", parsed_data)

        # Optional: Save parsed data to hotel folder (e.g., for dashboard)
        # For example:
        # hotel_folder = os.path.join(STORAGE_ROOT, hotel_id, "2025", utility_type)
        # os.makedirs(hotel_folder, exist_ok=True)
        # with open(os.path.join(hotel_folder, f"{job_id}.json"), "w") as f:
        #     json.dump(parsed_data, f)

    except Exception as e:
        save_job_status(job_id, "error", {"error": str(e)})

@router.get("/utilities/status/{job_id}")
async def check_parsing_status(job_id: str):
    status_path = os.path.join(STORAGE_ROOT, "jobs", job_id, "status.json")

    if not os.path.exists(status_path):
        raise HTTPException(status_code=404, detail="Job not found or still processing")

    with open(status_path, "r") as f:
        status_data = json.load(f)

    return {
        "status": status_data.get("status", "processing"),
        "data": status_data.get("data", None)
    }
