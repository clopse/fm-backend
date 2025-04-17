from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import shutil
import os
import json
from datetime import datetime

router = APIRouter()

BASE_DIR = Path("storage")

@router.post("/api/uploads/tenders")
async def upload_tender(
    file: UploadFile = File(...),
    hotel_id: str = Form(...),
    job_title: str = Form(...),
    status: str = Form(...)
):
    year = str(datetime.now().year)
    folder_path = BASE_DIR / hotel_id / year / "tenders"
    folder_path.mkdir(parents=True, exist_ok=True)

    safe_title = job_title.replace(" ", "_").replace("/", "-")
    file_ext = Path(file.filename).suffix
    file_path = folder_path / f"{safe_title}{file_ext}"
    meta_path = folder_path / f"{safe_title}.json"

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    metadata = {
        "job_title": job_title,
        "filename": file_path.name,
        "status": status,
        "uploaded_at": datetime.now().isoformat()
    }

    with open(meta_path, "w") as f:
        json.dump(metadata, f)

    return JSONResponse(content={"message": "Tender uploaded successfully."})

@router.get("/api/uploads/tenders")
async def list_tenders(hotel_id: str):
    year = str(datetime.now().year)
    folder_path = BASE_DIR / hotel_id / year / "tenders"
    if not folder_path.exists():
        return []

    files = []
    for f in folder_path.glob("*.json"):
        with open(f, "r") as meta_file:
            data = json.load(meta_file)
            files.append(data)
    return files

@router.delete("/api/uploads/tenders/delete")
async def delete_tender(hotel_id: str, filename: str):
    year = str(datetime.now().year)
    folder_path = BASE_DIR / hotel_id / year / "tenders"
    file_path = folder_path / filename
    meta_path = folder_path / (Path(filename).stem + ".json")

    if file_path.exists():
        file_path.unlink()
    if meta_path.exists():
        meta_path.unlink()

    return {"message": "Tender deleted successfully"}

@router.post("/api/uploads/tenders/update-status")
async def update_tender_status(hotel_id: str, filename: str, status: str):
    year = str(datetime.now().year)
    folder_path = BASE_DIR / hotel_id / year / "tenders"
    meta_path = folder_path / (Path(filename).stem + ".json")

    if not meta_path.exists():
        raise HTTPException(status_code=404, detail="Metadata not found")

    with open(meta_path, "r") as f:
        data = json.load(f)

    data["status"] = status

    with open(meta_path, "w") as f:
        json.dump(data, f)

    return {"message": "Status updated"}
