from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import os
import time
import boto3
import pdfplumber
from io import BytesIO

from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

def detect_bill_type_from_pdf(file_bytes: bytes) -> str:
    try:
        with pdfplumber.open(BytesIO(file_bytes)) as pdf:
            all_text = " ".join([page.extract_text() or "" for page in pdf.pages]).lower()
            if "mprn" in all_text or "mic" in all_text or "day units" in all_text:
                return "electricity"
            elif "gprn" in all_text or "therms" in all_text or "gas usage" in all_text:
                return "gas"
    except Exception as e:
        print(f"❌ Error reading PDF: {e}")
    return "electricity"  # fallback

@router.post("/utilities/precheck")
async def precheck_bill_type(file: UploadFile = File(...)):
    try:
        content = await file.read()
        bill_type = detect_bill_type_from_pdf(content)
        return {"bill_type": bill_type, "filename": file.filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Precheck error: {str(e)}")

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

def process_and_store_docupanda(db, content, hotel_id, utility_type, supplier, filename):
    try:
        print(f"\n📦 Submitting document {filename} to DocuPanda")
        encoded = base64.b64encode(content).decode()

        upload_payload = {
            "document": {"file": {"contents": encoded, "filename": filename}}
        }
        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json=upload_payload,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        print(f"📤 Upload response: {upload_res.status_code}")
        print(upload_res.text)

        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        if not document_id or not job_id:
            print("❌ Missing documentId or jobId.")
            return

        for attempt in range(10):
            time.sleep(6)
            res = requests.get(
                f"https://app.docupanda.io/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            )
            print(f"🕓 Job poll {attempt + 1}: {res.status_code}")
            job_status = res.json()
            print(job_status)
            if job_status.get("status") == "completed":
                break
            elif job_status.get("status") == "error":
                print("❌ Upload job failed.")
                return
        else:
            print("❌ Upload job timeout.")
            return

        for attempt in range(10):
            time.sleep(10)
            doc_check = requests.get(
                f"https://app.docupanda.io/document/{document_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            )
            print(f"📄 Document status check {attempt + 1}: {doc_check.status_code}")
            doc_data = doc_check.json()
            print(doc_data)
            if doc_data.get("status") == "ready":
                break
        else:
            print("❌ Document never reached 'ready' status.")
            return

        schema_id = SCHEMA_ELECTRICITY if utility_type == "electricity" else SCHEMA_GAS

        std_payload = {
            "documentIds": [document_id],
            "schemaId": schema_id,
            "forceRecompute": True
        }

        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json=std_payload,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        print(f"⚙️ Standardization request: {std_res.status_code}")
        print(std_res.text)
        std_data = std_res.json()
        std_id = std_data.get("standardizationId")
        if not std_id:
            print("❌ No standardizationId returned")
            return

        for attempt in range(10):
            time.sleep(6)
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            )
            print(f"🔁 Standardization poll {attempt + 1}: {result.status_code}")
            std_result = result.json()
            print(std_result)
            if std_result.get("status") == "completed":
                parsed = std_result.get("result", {})
                billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
                s3_path = save_json_to_s3(parsed, hotel_id, utility_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, utility_type, parsed, s3_path)
                print(f"✅ Bill parsed and saved: {s3_path}")
                return
            elif std_result.get("status") == "error":
                print(f"❌ Standardization error: {std_result}")
                return
        else:
            print("❌ Standardization polling timed out.")

    except Exception as e:
        print(f"❌ Error during parse-and-save: {e}")
