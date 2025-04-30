from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime
import os
import json

from app.utils.storage import save_file
from app.email.reader import parse_pdf
from app.schemas.utilities import UtilityUploadResponse
from app.utils.s3_utils import generate_filename_from_dates

router = APIRouter()

@router.post("/api/utilities/parse-pdf")
async def parse_utility_pdf(file: UploadFile = File(...)):
    try:
        content = await file.read()
        parsed = parse_pdf(content)
        return parsed
    except Exception as e:
        print(f"❌ PDF parsing error: {e}")
        raise HTTPException(status_code=400, detail=f"Parsing failed: {str(e)}")


@router.post("/api/utilities/save-corrected", response_model=UtilityUploadResponse)
async def save_corrected_utility_data(
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    billing_start: str = Form(...),
    billing_end: str = Form(...),
    total_kwh: float = Form(...),
    total_eur: float = Form(...),
    day_kwh: float = Form(None),
    night_kwh: float = Form(None),
    subtotal_eur: float = Form(None),
    confidence_score: int = Form(None),
    file: UploadFile = File(...)
):
    try:
        filename_base = generate_filename_from_dates(utility_type, billing_start, billing_end)

        year = str(datetime.now().year)
        pdf_filename = f"{filename_base}.pdf"
        json_filename = f"{filename_base}.json"

        # Save PDF
        pdf_path = save_file(file, hotel_id, "utilities", pdf_filename)

        # Save JSON
        metadata = {
            "utility_type": utility_type,
            "billing_start": billing_start,
            "billing_end": billing_end,
            "total_kwh": total_kwh,
            "total_eur": total_eur,
            "day_kwh": day_kwh,
            "night_kwh": night_kwh,
            "subtotal_eur": subtotal_eur,
            "confidence_score": confidence_score,
            "uploaded_at": datetime.utcnow().strftime("%Y-%m-%d")
        }

        json_path = pdf_path.replace(".pdf", ".json")

        with open(json_path, "w") as f:
            json.dump(metadata, f, indent=2)

        return UtilityUploadResponse(
            message="Utility bill uploaded and saved",
            file_path=pdf_path,
            metadata_path=json_path
        )

    except Exception as e:
        print(f"❌ Error saving corrected utility: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving corrected utility: {e}")
