from fastapi import APIRouter, UploadFile, Form
from app.utils.compliance_history import add_history_entry
from datetime import datetime
import shutil, os

router = APIRouter()

@router.post("/uploads/compliance")
async def upload_compliance_file(
    hotel_id: str = Form(...),
    task_id: str = Form(...),
    report_date: str = Form(...),
    file: UploadFile = Form(...)
):
    now = datetime.utcnow().isoformat()
    ext = os.path.splitext(file.filename)[-1]
    filename = f"{task_id}_{now[:10].replace('-', '')}{ext}"
    folder = f"storage/{hotel_id}/compliance/{task_id}"
    os.makedirs(folder, exist_ok=True)

    file_path = f"{folder}/{filename}"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    file_url = f"https://s3.amazonaws.com/jmk-project-uploads/{hotel_id}/compliance/{task_id}/{filename}"

    entry = {
        "fileName": filename,
        "reportDate": report_date,
        "uploadedAt": now,
        "uploadedBy": "SYSTEM",
        "fileUrl": file_url,
        "type": "upload"
    }
    add_history_entry(hotel_id, task_id, entry)

    return {"message": "File uploaded and history updated"}
