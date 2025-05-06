from fastapi import APIRouter, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
import os
from datetime import datetime
from uuid import uuid4

router = APIRouter()

UPLOAD_DIR = "storage/compliance"

@router.post("/uploads/compliance")
async def upload_compliance_file(
    hotel_id: str = Form(...),
    task_id: str = Form(...),
    report_date: str = Form(...),
    file: UploadFile = Form(...)
):
    try:
        # Validate date format
        parsed_date = datetime.strptime(report_date, "%Y-%m-%d")

        # Ensure folder exists
        hotel_folder = os.path.join(UPLOAD_DIR, hotel_id, task_id)
        os.makedirs(hotel_folder, exist_ok=True)

        # Generate unique filename
        ext = os.path.splitext(file.filename)[1]
        filename = f"{parsed_date.date()}_{uuid4().hex[:6]}{ext}"
        save_path = os.path.join(hotel_folder, filename)

        # Save file
        with open(save_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)

        return JSONResponse(content={"message": "File uploaded successfully", "filename": filename})

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
