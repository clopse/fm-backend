from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import os
import time
from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3, save_pdf_to_s3

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "bd3ec499"

def detect_bill_type(pages_text: list[str], supplier: str) -> str:
    joined = " ".join(pages_text).lower()
    supplier = supplier.lower()
    if "gprn" in joined or "therms" in joined or "gas usage" in joined or "flogas" in supplier:
        return "gas"
    elif "mprn" in joined or "mic" in joined or "day units" in joined or "arden" in supplier:
        return "electricity"
    return "electricity"  # fallback default

def send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_path):
    webhook_url = os.getenv("UPLOAD_WEBHOOK_URL")
    print(f"➡️ Using webhook URL: {webhook_url}")
    if not webhook_url:
        print("⚠️ No webhook URL set in .env")
        return
    try:
        payload = {
            "status": "success",
            "hotel_id": hotel_id,
            "bill_type": bill_type,
            "filename": filename,
            "billing_start": billing_start,
            "s3_path": s3_path,
            "timestamp": datetime.utcnow().isoformat()
        }
        res = requests.post(webhook_url, json=payload, timeout=5)
        print(f"📡 Webhook sent: {res.status_code}")
    except Exception as e:
        print(f"⚠️ Webhook failed: {e}")

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
        bill_type = detect_bill_type(pages_text, supplier)
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS
        print(f"🔎 Detected bill type: {bill_type} → using schema: {schema_id}")

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
        
        std_id_list = std_res.json().get("standardizationIds", [])
        std_id = std_id_list[0] if std_id_list else None
        
        if not std_id:
            print("❌ No standardizationId found in list.")
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
                time.sleep(2)
                std_check = requests.get(
                    f"https://app.docupanda.io/standardize/{std_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
                )
                std_json = std_check.json()
                print("🧾 Full standardization JSON:")
                print(std_json)

                parsed = std_json.get("result", {})
                if not parsed:
                    print("❌ Empty result — maybe schema mismatch?")
                    return

                billing_start = (
                    parsed.get("billingPeriod", {}).get("startDate")
                    or parsed.get("billingPeriodStartDate")
                    or datetime.utcnow().strftime("%Y-%m-%d")
                )

                s3_json_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
                save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_json_path)
                print("🧠 Parsed data saved to DB")
                print("🚀 Sending webhook now")
                send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_json_path)
                print(f"✅ Done — file saved and webhook sent")
                return

            elif status == "error":
                print("❌ Standardization failed.")
                return

        print("❌ Standardization polling timed out.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Unexpected processing error: {e}")
