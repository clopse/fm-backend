from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
import base64
import requests
import os
import time
from datetime import datetime
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "bd3ec499"

# Detect bill type from pages text
def detect_bill_type(pages_text: list[str]) -> str:
    joined = " ".join(pages_text).lower()
    if "mprn" in joined or "mic" in joined or "day units" in joined:
        return "electricity"
    elif "gprn" in joined or "therms" in joined or "gas usage" in joined:
        return "gas"
    return "electricity"  # default fallback

def process_and_store_docupanda(db, content, hotel_id, utility_type, supplier, filename):
    try:
        encoded = base64.b64encode(content).decode()

        # Upload document
        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        if not document_id or not job_id:
            print("❌ No documentId or jobId returned")
            return

        # Poll for job completion (use 30s delay before first check)
        max_attempts = 20
        time.sleep(30)  # Initial wait before first check
        for attempt in range(max_attempts):
            time.sleep(min(2 ** attempt, 60))
            job_status = requests.get(
                f"https://app.docupanda.io/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            if job_status.get("status") == "completed":
                break
            elif job_status.get("status") == "error":
                print("❌ DocuPanda job errored")
                return
        else:
            print("❌ Job polling timeout")
            return

        # Wait before standardize step
        time.sleep(5)

        # Get document plain text
        doc_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()
        pages_text = doc_res.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS

        # Standardize document
        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        std_id = std_res.json().get("standardizationId")
        if not std_id:
            print("❌ No standardizationId returned")
            return

        # Poll for standardization
        for attempt in range(max_attempts):
            time.sleep(min(2 ** attempt, 60))
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            if result.get("status") == "completed":
                parsed = result.get("result", {})
                billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
                s3_path = save_json_to_s3(parsed, hotel_id, utility_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, utility_type, parsed, s3_path)
                print(f"✅ Parsed and saved: {s3_path}")
                return
            elif result.get("status") == "error":
                print("❌ Standardization errored")
                return

        print("❌ Standardization polling timeout")

    except Exception as e:
        print(f"❌ Background parsing error: {str(e)}")

@router.post("/utilities/parse-and-save")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    supplier: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    content = await file.read()
    filename = file.filename

    background_tasks.add_task(
        process_and_store_docupanda,
        db, content, hotel_id, utility_type, supplier, filename
    )

    return {"status": "processing", "message": "Upload received. Processing in background."}
