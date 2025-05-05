from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import os
import time
import json

from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "bd3ec499"

# Detect bill type based on keywords
def detect_bill_type(pages_text: list[str]) -> str:
    joined = " ".join(pages_text).lower()
    if "mprn" in joined or "mic" in joined or "day units" in joined:
        return "electricity"
    elif "gprn" in joined or "therms" in joined or "gas usage" in joined:
        return "gas"
    return "unknown"

@router.post("/utilities/precheck")
async def precheck_bill_type(file: UploadFile = File(...)):
    try:
        content = await file.read()
        encoded = base64.b64encode(content).decode()

        # Step 1: Upload to DocuPanda
        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": file.filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )

        if upload_res.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to upload to DocuPanda")

        doc_data = upload_res.json()
        document_id = doc_data.get("documentId")
        if not document_id:
            raise HTTPException(status_code=400, detail="DocuPanda did not return a documentId")

        # Step 2: Fetch page text
        doc_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()

        pages_text = doc_res.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)

        return {"bill_type": bill_type, "filename": file.filename}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def process_and_store_docupanda(db, content, hotel_id, utility_type, supplier, filename):
    try:
        encoded = base64.b64encode(content).decode()

        # Step 1: Upload document
        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        print(f"DocuPanda Upload Response: {upload_res.status_code} - {upload_res.text}")
        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")

        if not document_id or not job_id:
            print(f"❌ Error: No documentId or jobId returned: {upload_res.text}")
            return

        # Step 2: Poll for parsing
        max_attempts = 5
        for attempt in range(max_attempts):
            time.sleep(min(2 ** attempt, 60))
            job_status_res = requests.get(
                f"https://app.docupanda.io/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()

            status = job_status_res.get("status")
            print(f"Polling attempt {attempt+1}: Status - {status}")

            if status == "completed":
                break
            elif status == "error":
                print(f"❌ Job processing failed for {filename}.")
                return
        else:
            print("❌ Job polling timed out.")
            return

        # Step 3: Get text to detect bill type
        doc_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()

        pages_text = doc_res.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS

        # Step 4: Standardize
        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )

        print(f"Standardize response: {std_res.status_code} - {std_res.text}")
        std_id = std_res.json().get("standardizationId")
        if not std_id:
            print(f"❌ No standardizationId returned for {filename}")
            return

        # Step 5: Poll for standardization
        for attempt in range(max_attempts):
            time.sleep(min(2 ** attempt, 60))
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()

            if result.get("status") == "completed":
                parsed = result.get("result", {})
                billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
                s3_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_path)
                print(f"✅ Parsed and saved: {s3_path}")
                return
            elif result.get("status") == "error":
                print(f"❌ Standardization failed for {filename}")
                return

        print("❌ Standardization polling timed out")

    except Exception as e:
        print(f"❌ Error processing document: {str(e)}")

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
