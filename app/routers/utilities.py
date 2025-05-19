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
UPLOAD_WEBHOOK_URL = os.getenv("UPLOAD_WEBHOOK_URL")


def detect_bill_type(pages_text: list[str], supplier: str) -> str:
    """
    Improved bill type detection with more comprehensive keyword matching
    """
    # Join all pages and convert to lowercase for easier matching
    full_text = " ".join(pages_text).lower()
    
    # Gas indicators (more comprehensive)
    gas_keywords = [
        "mprn",  # Meter Point Reference Number for gas
        "gas usage", "gas supply", "gas bill", "gas account",
        "therms", "cubic feet", "cu ft", "ccf",
        "calorific value", "volume correction",
        "gas transportation", "gas distribution",
        "standing charge gas", "gas standing charge",
        "gas unit rate", "gas charges",
        "natural gas", "lpg", "liquid petroleum gas"
    ]
    
    # Electricity indicators (more comprehensive) 
    electricity_keywords = [
        "mpan",  # Meter Point Administration Number for electricity
        "electricity usage", "electricity supply", "electricity bill", "electricity account",
        "kwh", "kilowatt", "kilowatt hour", "kw hr",
        "day units", "night units", "peak units", "off-peak units",
        "electricity transportation", "electricity distribution",
        "standing charge electricity", "electricity standing charge", 
        "electricity unit rate", "electricity charges",
        "import", "export", "generation"
    ]
    
    # Count keyword matches
    gas_matches = sum(1 for keyword in gas_keywords if keyword in full_text)
    electricity_matches = sum(1 for keyword in electricity_keywords if keyword in full_text)
    
    print(f"🔍 Detection results:")
    print(f"   Gas keywords found: {gas_matches}")
    print(f"   Electricity keywords found: {electricity_matches}")
    print(f"   Supplier: {supplier}")
    
    # Decision logic - prioritize document content over supplier name
    if gas_matches > electricity_matches and gas_matches > 0:
        return "gas"
    elif electricity_matches > gas_matches and electricity_matches > 0:
        return "electricity"
    elif gas_matches == electricity_matches and gas_matches > 0:
        # If tied, use supplier name as tiebreaker
        if any(gas_term in supplier.lower() for gas_term in ["gas", "flogas", "lpg"]):
            return "gas"
        else:
            return "electricity"
    else:
        # No clear indicators found, use supplier as fallback
        if any(gas_term in supplier.lower() for gas_term in ["gas", "flogas", "lpg"]):
            return "gas"
        else:
            # Default to electricity if no clear indicators
            print("⚠️ No clear bill type indicators found, defaulting to electricity")
            return "electricity"


def send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_path):
    if not UPLOAD_WEBHOOK_URL:
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
        res = requests.post(UPLOAD_WEBHOOK_URL, json=payload, timeout=5)
        print(f"📡 Webhook sent: {res.status_code}")
    except Exception as e:
        print(f"⚠️ Webhook failed: {e}")


import PyPDF2
from io import BytesIO

@router.post("/utilities/precheck")
async def precheck_file(
    file: UploadFile = File(...),
    supplier: str = Form(...)
):
    """
    Quick precheck to validate file and detect bill type using our own PDF parser
    """
    try:
        # Check file type
        if not file.filename.lower().endswith('.pdf'):
            return {"valid": False, "error": "Only PDF files are supported"}
        
        # Check file size (e.g., max 10MB)
        content = await file.read()
        if len(content) > 10 * 1024 * 1024:  # 10MB limit
            return {"valid": False, "error": "File too large (max 10MB)"}
        
        print(f"\n🔍 PRECHECK: Starting for {file.filename} with supplier: {supplier}")
        
        # Extract text using PyPDF2 (fast local extraction)
        bill_type = "unknown"
        try:
            pdf_stream = BytesIO(content)
            pdf_reader = PyPDF2.PdfReader(pdf_stream)
            
            # Extract text from all pages
            pages_text = []
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                text = page.extract_text()
                pages_text.append(text)
                print(f"🔍 PRECHECK: Page {page_num + 1} text sample: {text[:200]}...")
            
            if pages_text:
                bill_type = detect_bill_type(pages_text, supplier)
                print(f"🔍 PRECHECK: Detected bill type: {bill_type}")
            else:
                print("🔍 PRECHECK: No text extracted from PDF")
                
        except Exception as e:
            print(f"⚠️ PRECHECK: PDF text extraction failed: {e}")
            # Try fallback - just use supplier name for detection
            if any(gas_term in supplier.lower() for gas_term in ["gas", "flogas", "lpg"]):
                bill_type = "gas"
                print(f"🔍 PRECHECK: Fallback detection based on supplier: {bill_type}")
        
        # Reset file position for potential future reads
        file.file.seek(0)
        
        print(f"🔍 PRECHECK: Final result - bill_type: {bill_type}")
        
        return {
            "valid": True,
            "filename": file.filename,
            "size": len(content),
            "bill_type": bill_type,
            "supplier": supplier,
            "message": f"File passed precheck - detected as {bill_type} bill"
        }
    except Exception as e:
        print(f"❌ PRECHECK: Failed with exception: {e}")
        return {"valid": False, "error": f"Precheck failed: {str(e)}"}


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

        # Poll job status (document processing)
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

        # Fetch document details
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

        # Wait longer before polling standardization
        print("⏳ Waiting 10 seconds before polling standardization result...")
        time.sleep(10)

        for attempt in range(10):
            std_check = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            )
            std_json = std_check.json()
            status = std_json.get("status")
            print(f"🔁 Poll {attempt + 1}: {status}")

            if status == "completed":
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
                print(f"✅ Saved to S3 and DB: {s3_json_path}")

                send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_json_path)
                return

            elif status == "error":
                print("❌ Standardization failed.")
                return
            else:
                time.sleep(10)

        print("❌ Standardization polling timed out.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Unexpected processing error: {e}")
