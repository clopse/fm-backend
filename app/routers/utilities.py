from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import os
import time
import pdfplumber
from io import BytesIO
from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3, save_pdf_to_s3

router = APIRouter()

DOCUPIPE_API_KEY = os.getenv("DOCUPIPE_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b44"
UPLOAD_WEBHOOK_URL = os.getenv("UPLOAD_WEBHOOK_URL")


def detect_supplier_from_text(pages_text):
    full_text = " ".join(pages_text).lower()
    if "flogas" in full_text or "fgnc" in full_text:
        return "Flogas"
    elif "arden" in full_text or "aes916" in full_text:
        return "Arden Energy"
    else:
        return "Unknown"


def detect_bill_type(pages_text, supplier):
    full_text = " ".join(pages_text).lower()
    
    gas_keywords = ["mprn", "gas usage", "therms", "cubic feet", "calorific value"]
    electricity_keywords = ["mpan", "kwh", "kilowatt", "day units", "night units", "electricity"]
    
    gas_matches = sum(1 for keyword in gas_keywords if keyword in full_text)
    electricity_matches = sum(1 for keyword in electricity_keywords if keyword in full_text)
    
    print(f"Gas: {gas_matches}, Electricity: {electricity_matches}")
    
    if gas_matches > electricity_matches:
        return "gas"
    elif electricity_matches > gas_matches:
        return "electricity"
    else:
        if "flogas" in supplier.lower():
            return "gas"
        else:
            return "electricity"


def send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_path, supplier="Unknown"):
    if not UPLOAD_WEBHOOK_URL:
        print("No webhook URL set")
        return
    
    try:
        payload = {
            "status": "success",
            "hotel_id": hotel_id,
            "supplier": supplier,
            "bill_type": bill_type,
            "filename": filename,
            "billing_start": billing_start,
            "s3_path": s3_path,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        res = requests.post(UPLOAD_WEBHOOK_URL, json=payload, timeout=10)
        print(f"Webhook sent: {res.status_code}")
    except Exception as e:
        print(f"Webhook failed: {e}")


@router.post("/utilities/parse-and-save")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    file: UploadFile = File(...),
    bill_date: str = Form(...),
    db: Session = Depends(get_db)
):
    content = await file.read()
    filename = file.filename
    
    background_tasks.add_task(process_and_store_docupipe, db, content, hotel_id, filename, bill_date)
    return {"status": "processing", "message": "Upload received. Processing in background."}


def process_and_store_docupipe(db, content, hotel_id, filename, bill_date):
    try:
        print(f"Processing {filename}")
        
        # Detect supplier and bill type
        supplier = "Unknown"
        bill_type = "unknown"
        
        try:
            with pdfplumber.open(BytesIO(content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                
                if pages_text:
                    supplier = detect_supplier_from_text(pages_text)
                    bill_type = detect_bill_type(pages_text, supplier)
                    print(f"Detected: {supplier} - {bill_type}")
        except Exception as e:
            print(f"Local parsing failed: {e}")
        
        # Upload to DocuPipe
        encoded = base64.b64encode(content).decode()
        
        upload_res = requests.post(
            "https://app.docupipe.ai/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPIPE_API_KEY,
            },
        )
        print(f"DocuPipe upload: {upload_res.status_code}")
        
        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        
        if not document_id or not job_id:
            print("Missing documentId or jobId")
            return
        
        # Wait for document processing
        for attempt in range(10):
            time.sleep(5)
            res = requests.get(
                f"https://app.docupipe.ai/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
            )
            status = res.json().get("status")
            print(f"Document processing {attempt + 1}: {status}")
            if status == "completed":
                break
            elif status == "error":
                print("Document processing failed")
                return
        else:
            print("Document processing timeout")
            return
        
        # Start standardization
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS
        print(f"Using schema: {schema_id} for {bill_type}")
        
        std_res = requests.post(
            "https://app.docupipe.ai/v2/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPIPE_API_KEY,
            },
        )
        print(f"Standardization started: {std_res.status_code}")
        
        std_data = std_res.json()
        std_job_id = std_data.get("jobId")
        std_id = std_data.get("standardizationIds", [None])[0]
        
        if not std_job_id or not std_id:
            print("Missing standardization IDs")
            return
        
        # Wait for standardization
        print("Waiting for standardization...")
        time.sleep(15)
        
        for attempt in range(12):
            std_job_res = requests.get(
                f"https://app.docupipe.ai/job/{std_job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
            )
            status = std_job_res.json().get("status")
            print(f"Standardization {attempt + 1}: {status}")
            
            if status == "completed":
                # Get final result
                result_res = requests.get(
                    f"https://app.docupipe.ai/standardize/{std_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                )
                result_json = result_res.json()
                parsed = result_json.get("result", {})
                
                if not parsed:
                    parsed = result_json.get("data", {})
                    if not parsed:
                        print("Empty result")
                        print(f"Full response: {result_json}")
                        return
                
                print(f"Got parsed data with keys: {list(parsed.keys())}")
                
                billing_start = parsed.get("billingPeriod", {}).get("startDate") or parsed.get("billingPeriodStartDate") or bill_date
                
                # Save to S3 and database
                s3_json_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
                save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_json_path)
                
                print(f"Saved: {s3_json_path}")
                
                # Send webhook
                send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_json_path, supplier)
                return
                
            elif status == "error":
                print("Standardization failed")
                return
            else:
                time.sleep(10)
        
        print("Standardization timeout")
        
    except Exception as e:
        print(f"Processing error: {e}")
        import traceback
        traceback.print_exc()
