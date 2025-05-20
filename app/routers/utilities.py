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

# Update the API endpoints to DocuPipe with correct schema IDs
DOCUPIPE_API_KEY = os.getenv("DOCUPIPE_API_KEY")  # Consider renaming env var
SCHEMA_ELECTRICITY = "3ca991a9"  # Electricity Bill schema ID from DocuPipe
SCHEMA_GAS = "33093b44"  # Gas Bill schema ID from DocuPipe (updated!)
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


def send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_path, supplier="Unknown"):
    if not UPLOAD_WEBHOOK_URL:
        print("⚠️ No webhook URL configured (set UPLOAD_WEBHOOK_URL in .env)")
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
            "timestamp": datetime.utcnow().isoformat(),
            "processed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        }
        
        print(f"📡 Sending webhook to: {UPLOAD_WEBHOOK_URL}")
        print(f"📡 Webhook payload: {payload}")
        
        res = requests.post(
            UPLOAD_WEBHOOK_URL, 
            json=payload, 
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        print(f"📡 Webhook response: {res.status_code}")
        if res.status_code != 200:
            print(f"📡 Webhook response body: {res.text}")
        else:
            print("📡 Webhook sent successfully!")
            
    except requests.exceptions.Timeout:
        print(f"⚠️ Webhook timeout after 10 seconds")
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Webhook request failed: {e}")
    except Exception as e:
        print(f"⚠️ Webhook error: {e}")


import pdfplumber
from io import BytesIO

@router.post("/utilities/precheck")
async def precheck_file(
    file: UploadFile = File(...),
    supplier: str = Form(default="Unknown")
):
    """
    Quick precheck to validate file and detect bill type using pdfplumber
    """
    try:
        print(f"\n🔍 PRECHECK: Received file: {file.filename}, supplier: {supplier}")
        
        # Check file type
        if not file.filename or not file.filename.lower().endswith('.pdf'):
            return {"valid": False, "error": "Only PDF files are supported"}
        
        # Check file size (e.g., max 10MB)
        content = await file.read()
        if len(content) > 10 * 1024 * 1024:  # 10MB limit
            return {"valid": False, "error": "File too large (max 10MB)"}
        
        print(f"🔍 PRECHECK: Starting for {file.filename} with supplier: {supplier}")
        
        # Extract text using pdfplumber (fast local extraction)
        bill_type = "unknown"
        try:
            # Open PDF from bytes
            with pdfplumber.open(BytesIO(content)) as pdf:
                # Extract text from all pages
                pages_text = []
                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                        print(f"🔍 PRECHECK: Page {page_num + 1} text sample: {text[:200]}...")
                    else:
                        print(f"🔍 PRECHECK: Page {page_num + 1} - no text extracted")
                
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
            else:
                bill_type = "electricity"
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
        import traceback
        traceback.print_exc()
        return {"valid": False, "error": f"Precheck failed: {str(e)}"}


def detect_supplier_from_text(pages_text: list[str]) -> str:
    """
    Detect supplier from PDF text content
    """
    full_text = " ".join(pages_text).lower()
    
    # Check for supplier keywords in text
    if "flogas" in full_text or "fgnc" in full_text:
        return "Flogas"
    elif "arden" in full_text or "aes916" in full_text:
        return "Arden Energy"
    elif "electric ireland" in full_text or "eir" in full_text:
        return "Electric Ireland"
    elif "bord gais" in full_text or "bord gas" in full_text:
        return "Bord Gais"
    else:
        return "Unknown"


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

    background_tasks.add_task(
        process_and_store_docupipe,
        db, content, hotel_id, filename, bill_date
    )
    return {"status": "processing", "message": "Upload received. Processing in background."}


def process_and_store_docupipe(db, content, hotel_id, filename, bill_date):
    try:
        print(f"\n📤 Processing {filename}")
        
        # First, extract text locally with pdfplumber to detect supplier and bill type
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
                    print(f"🔍 Detected: {supplier} - {bill_type}")
        except Exception as e:
            print(f"⚠️ Local parsing failed: {e}")
        
        # Continue with DocuPipe processing
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
        print(f"📤 DocuPipe upload: {upload_res.status_code}")

        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        
        if not document_id or not job_id:
            print("❌ Missing documentId or jobId")
            return

        # Wait for document processing
        for attempt in range(10):
            time.sleep(5)
            res = requests.get(
                f"https://app.docupipe.ai/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
            )
            status = res.json().get("status")
            print(f"🕓 Document processing {attempt + 1}: {status}")
            if status == "completed":
                break
            elif status == "error":
                print("❌ Document processing failed")
                return
        else:
            print("❌ Document processing timeout")
            return

        # Select schema based on detected bill type
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS
        print(f"🔎 Using schema: {schema_id} for {bill_type}")

        # Start standardization
        std_res = requests.post(
            "https://app.docupipe.ai/v2/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPIPE_API_KEY,
            },
        )
        print(f"⚙️ Standardization started: {std_res.status_code}")

        std_data = std_res.json()
        std_job_id = std_data.get("jobId")
        std_id = std_data.get("standardizationIds", [None])[0]

        if not std_job_id or not std_id:
            print("❌ Missing standardization IDs")
            return

        # Wait for standardization
        print("⏳ Waiting for standardization...")
        time.sleep(15)

        for attempt in range(12):  # 2 minutes max
            std_job_res = requests.get(
                f"https://app.docupipe.ai/job/{std_job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
            )
            status = std_job_res.json().get("status")
            print(f"🔁 Standardization {attempt + 1}: {status}")

            if status == "completed":
                # Get final result
                result_res = requests.get(
                    f"https://app.docupipe.ai/standardize/{std_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                )
                parsed = result_res.json().get("result", {})

                if parsed:
                    # Extract billing date
                    billing_start = (
                        parsed.get("billingPeriod", {}).get("startDate")
                        or parsed.get("billingPeriodStartDate")
                        or bill_date
                    )

                    # Save to S3 and database
                    s3_json_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
                    save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                    save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_json_path)
                    
                    print(f"✅ Saved: {s3_json_path}")

                    # Send webhook with all the details
                    send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_json_path, supplier)
                    return
                else:
                    print("❌ Empty result")
                    return

            elif status == "error":
                print("❌ Standardization failed")
                return
            else:
                time.sleep(10)

        print("❌ Standardization timeout")

    except Exception as e:
        print(f"❌ Processing error: {e}")
        import traceback
        traceback.print_exc()
