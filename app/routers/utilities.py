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

# Update the API endpoints to DocuPipe
DOCUPIPE_API_KEY = os.getenv("DOCUPIPE_API_KEY")  # Consider renaming env var
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


@router.post("/utilities/parse-and-save")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    supplier: str = Form(...),
    bill_type: str = Form(default=""),  # Accept bill_type from precheck
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    content = await file.read()
    filename = file.filename

    # Pass the detected bill_type to the background task
    background_tasks.add_task(
        process_and_store_docupanda,
        db, content, hotel_id, supplier, filename, bill_type
    )
    return {"status": "processing", "message": "Upload received. Processing in background."}


def process_and_store_docupanda(db, content, hotel_id, supplier, filename, precheck_bill_type=""):
    try:
        print(f"\n📤 Uploading {filename} to DocuPipe")  # Updated message
        print(f"📋 Precheck result: bill_type = {precheck_bill_type}")
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
        print(f"📤 Upload response: {upload_res.status_code}")
        print(upload_res.text)

        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        if not document_id or not job_id:
            print("❌ Missing documentId or jobId.")
            return

        # Poll job status (document processing)
        # Reduce polling attempts to prevent timeout
        for attempt in range(6):  # Reduced from 10 to 6 attempts
            time.sleep(5)
            res = requests.get(
                f"https://app.docupipe.ai/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
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

        # Use precheck bill type if available, otherwise detect from DocuPanda text
        bill_type = "unknown"
        if precheck_bill_type and precheck_bill_type in ["gas", "electricity"]:
            bill_type = precheck_bill_type
            print(f"✅ Using precheck bill type: {bill_type}")
        else:
            # Fallback: detect from DocuPanda text
            print("🔍 No precheck result, detecting from DocuPanda text...")
            doc_res = requests.get(
                f"https://app.docupipe.ai/document/{document_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
            )
            doc_json = doc_res.json()
            print(f"📄 DocuPipe response structure: {list(doc_json.keys())}")
            
            # Handle both DocuPanda and DocuPipe response formats
            pages_text = []
            result = doc_json.get("result", {})
            
            # Try DocuPanda format first (pagesText)
            if "pagesText" in result:
                pages_text = result["pagesText"]
                print(f"📄 Found pagesText format with {len(pages_text)} pages")
            # Try DocuPipe format (pages array)
            elif "pages" in result:
                pages_text = [page.get("text", "") for page in result["pages"]]
                print(f"📄 Found pages array format with {len(pages_text)} pages")
            # Try single text field
            elif "text" in result:
                pages_text = [result["text"]]
                print(f"📄 Found single text format")
            else:
                print(f"❌ Unexpected response format. Available keys: {list(result.keys())}")
            
            if pages_text:
                print(f"📄 Sample text: {pages_text[0][:200] if pages_text[0] else 'EMPTY'}...")
                bill_type = detect_bill_type(pages_text, supplier)
                print(f"🔍 Fallback detection result: {bill_type}")
            else:
                print("❌ No text found in any format")

        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS
        print(f"🔎 Final bill type: {bill_type} → using schema: {schema_id}")

        # Try both v1 and v2 standardize endpoints
        standardize_endpoints = [
            "https://app.docupipe.ai/v2/standardize/batch",    # DocuPipe v2 (preferred)
            "https://app.docupipe.ai/standardize/batch"        # DocuPipe v1 (fallback)
        ]
        
        std_res = None
        for endpoint in standardize_endpoints:
            print(f"⚙️ Trying standardization endpoint: {endpoint}")
            std_res = requests.post(
                endpoint,
                json={"documentIds": [document_id], "schemaId": schema_id},
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                    "X-API-Key": DOCUPIPE_API_KEY,
                },
            )
            print(f"⚙️ Standardization response {std_res.status_code}: {std_res.text}")
            if std_res.status_code == 200:
                break
        
        if not std_res or std_res.status_code != 200:
            print("❌ All standardization endpoints failed")
            return

        std_id_list = std_res.json().get("standardizationIds", [])
        std_id = std_id_list[0] if std_id_list else None

        if not std_id:
            print("❌ No standardizationId found in list.")
            return

        # Get the job ID from the standardization response
        std_job_id = std_res.json().get("jobId")
        if not std_job_id:
            print("❌ No jobId found in standardization response.")
            return

        # Wait a bit before polling standardization job
        print("⏳ Waiting 10 seconds before polling standardization job...")
        time.sleep(10)

        # Poll the standardization JOB status (not the result directly)
        for attempt in range(10):
            std_job_check = requests.get(
                f"https://app.docupipe.ai/job/{std_job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
            )
            std_job_json = std_job_check.json()
            status = std_job_json.get("status")
            print(f"🔁 Standardization job poll {attempt + 1}: {status}")

            if status == "completed":
                # NOW fetch the final result from the std_id
                print("✅ Standardization job completed, fetching result...")
                std_check = requests.get(
                    f"https://app.docupipe.ai/standardize/{std_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                )
                std_json = std_check.json()
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
                print("❌ Standardization job failed.")
                return
            else:
                # Wait between polls
                time.sleep(8)

        print("❌ Standardization job polling timed out.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Unexpected processing error: {e}")
