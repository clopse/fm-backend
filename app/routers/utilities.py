from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import os
import time
from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db, get_utility_data_for_year
from app.utils.s3 import save_json_to_s3, save_pdf_to_s3

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "bd3ec499"

def detect_bill_type(pages_text: list[str]) -> str:
    joined = " ".join(pages_text).lower()
    if "mprn" in joined or "mic" in joined or "day units" in joined:
        return "electricity"
    elif "gprn" in joined or "therms" in joined or "gas usage" in joined:
        return "gas"
    return "electricity"

@router.post("/utilities/parse-and-save")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    supplier: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    content = await file.read()
    filename = file.filename

    background_tasks.add_task(
        process_and_store_docupanda,
        db, content, hotel_id, supplier, filename
    )
    return {"status": "processing", "message": "Upload received. Processing in background."}

def process_and_store_docupanda(db, content, hotel_id, supplier, filename):
    try:
        print(f"\n📤 Uploading {filename} to DocuPanda")
        encoded = base64.b64encode(content).decode()

        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
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
            time.sleep(5)
            res = requests.get(
                f"https://app.docupanda.io/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            )
            status = res.json().get("status")
            print(f"🕓 Job poll {attempt + 1}: {status}")
            if status == "completed":
                break
            elif status == "error":
                print("❌ Job failed.")
                return
        else:
            print("❌ Job polling timed out.")
            return

        doc_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        )
        doc_json = doc_res.json()
        pages_text = doc_json.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS

        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        print(f"⚙️ Standardization request: {std_res.status_code}")
        print(std_res.text)
        std_id = std_res.json().get("standardizationId")
        if not std_id:
            print("❌ No standardizationId returned.")
            return

        for attempt in range(10):
            time.sleep(5)
            std_check = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            )
            std_json = std_check.json()
            status = std_json.get("status")
            print(f"🔁 Poll {attempt + 1}: {status}")
            if status == "completed":
                parsed = std_json.get("result", {})
                billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
                s3_json_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
                save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_json_path)
                print(f"✅ Saved to S3 and DB: {s3_json_path}")
                return
            elif status == "error":
                print("❌ Standardization failed.")
                return

        print("❌ Standardization polling timed out.")

    except Exception as e:
        print(f"❌ Exception during processing: {e}")


@router.get("/api/{hotel_id}/utilities/{year}")
def get_utilities(hotel_id: str, year: int, db: Session = Depends(get_db)):
    try:
        results = get_utility_data_for_year(db, hotel_id, year)
        return {
            "status": "success",
            "data": [
                {
                    "billing_start": r.billing_start,
                    "customer_ref": r.customer_ref,
                    "billing_ref": r.billing_ref,
                    "meter_number": r.meter_number,
                    "total_amount": r.total_amount,
                    "s3_path": r.s3_path,
                }
                for r in results
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch utility data: {e}")
