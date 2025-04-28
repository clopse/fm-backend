from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime
import os
import json

from app.database import get_db
from app.utils.storage import save_file
from app.schemas.utilities import UtilityUploadResponse  # ✅ Pydantic model

router = APIRouter()

@router.post("/uploads/utilities", response_model=UtilityUploadResponse)
async def upload_utility_file(
    hotel_id: str = Form(...),
    utility_type: str = Form(...),  # e.g., "electric", "gas", "water"
    bill_date: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    file_path = save_file(file, hotel_id, "utilities", utility_type)

    metadata = {
        "utility_type": utility_type,
        "bill_date": bill_date,
        "uploaded_at": datetime.utcnow().strftime("%Y-%m-%d"),
        "filename": file.filename
    }

    json_filename = os.path.splitext(os.path.basename(file_path))[0] + ".json"
    json_path = os.path.join(os.path.dirname(file_path), json_filename)

    with open(json_path, "w") as f:
        json.dump(metadata, f, indent=2)

    return UtilityUploadResponse(
        message="Utility bill uploaded",
        file_path=file_path,
        metadata_path=json_path
    )
