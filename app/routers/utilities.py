from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime
from typing import Optional
import json
from io import BytesIO
import os


from app.parsers.arden import parse_arden
from app.parsers.flogas import parse_flogas
from app.utils.normalize import normalize_fields
from app.models.utility_bill import UtilityBill
from app.schemas.utilities import UtilityUploadResponse
from app.utils.s3_utils import upload_to_s3, generate_filename_from_dates

router = APIRouter()

@router.post("/api/utilities/parse-and-save", response_model=UtilityUploadResponse)
async def parse_and_save(
    file: UploadFile = File(...),
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    supplier: str = Form(...),
    billing_start: str = Form(...),
    billing_end: str = Form(...)
):
    try:
        content = await file.read()

        # Parse PDF
        if supplier == "arden" and utility_type == "electricity":
            raw = parse_arden(content)
        elif supplier == "flogas" and utility_type == "gas":
            raw = parse_flogas(content)
        else:
            raise HTTPException(status_code=400, detail="Unsupported supplier/utility")

        normalized = normalize_fields(raw, utility_type)
        parsed = UtilityBill(
            **normalized,
            hotel_id=hotel_id,
            utility_type=utility_type,
            supplier=supplier,
            raw_data=raw
        )

        # S3 Save
        year = billing_start[:4]
        folder = f"{hotel_id}/{year}/{utility_type}"
        base = generate_filename_from_dates(utility_type, billing_start, billing_end)
        pdf_key = f"{folder}/{base}.pdf"
        json_key = f"{folder}/{base}.json"

        upload_to_s3(content, pdf_key)
        json_bytes = BytesIO(json.dumps(parsed.dict(), indent=2).encode("utf-8"))
        upload_to_s3(json_bytes.read(), json_key)

        return UtilityUploadResponse(
            message="Utility bill uploaded and parsed",
            file_path=pdf_key,
            metadata_path=json_key,
            parsed_data=parsed.dict()  # ✅ Return full parsed data for frontend auto-fill
        )

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
