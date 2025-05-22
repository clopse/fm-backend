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

# Add this missing endpoint
@router.post("/utilities/precheck")
async def precheck_bill_type(file: UploadFile = File(...)):
    """Quick bill type detection for frontend preview"""
    try:
        content = await file.read()
        
        with pdfplumber.open(BytesIO(content)) as pdf:
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            
            if not pages_text:
                return {"bill_type": "unknown", "confidence": "low"}
            
            supplier = detect_supplier_from_text(pages_text)
            bill_type = detect_bill_type(pages_text, supplier)
            
            return {
                "bill_type": bill_type,
                "supplier": supplier,
                "confidence": "high" if bill_type != "unknown" else "low"
            }
            
    except Exception as e:
        print(f"Precheck error: {e}")
        return {"bill_type": "unknown", "confidence": "low", "error": str(e)}


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
            return "unknown"  # Changed from "electricity" to "unknown" for ambiguous cases


def send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_path, supplier="Unknown", status="success", error=None):
    if not UPLOAD_WEBHOOK_URL:
        print("No webhook URL set")
        return
    
    try:
        payload = {
            "status": status,
            "hotel_id": hotel_id,
            "supplier": supplier,
            "bill_type": bill_type,
            "filename": filename,
            "billing_start": billing_start,
            "s3_path": s3_path,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if error:
            payload["error"] = error
        
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
    bill_type: str = Form(...),  # ADD THIS - it was missing!
    supplier: str = Form(default="docupanda"),  # Make this optional with default
    db: Session = Depends(get_db)
):
    # Validate inputs
    if not DOCUPIPE_API_KEY:
        raise HTTPException(status_code=500, detail="DocuPipe API key not configured")
    
    if bill_type not in ["electricity", "gas"]:
        raise HTTPException(status_code=400, detail="Invalid bill type. Must be 'electricity' or 'gas'")
    
    content = await file.read()
    filename = file.filename
    
    # Validate file
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    
    if not filename or not filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    background_tasks.add_task(
        process_and_store_docupipe, 
        db, content, hotel_id, filename, bill_date, bill_type, supplier
    )
    
    return {"status": "processing", "message": "Upload received. Processing in background."}


def process_and_store_docupipe(db, content, hotel_id, filename, bill_date, bill_type, supplier="docupanda"):
    try:
        print(f"Processing {filename} - Type: {bill_type}, Hotel: {hotel_id}")
        
        # Verify bill type with local detection as backup
        detected_supplier = "Unknown"
        detected_bill_type = bill_type  # Use frontend selection as primary
        
        try:
            with pdfplumber.open(BytesIO(content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                
                if pages_text:
                    detected_supplier = detect_supplier_from_text(pages_text)
                    local_detection = detect_bill_type(pages_text, detected_supplier)
                    
                    # Warn if detection differs from user selection
                    if local_detection != bill_type and local_detection != "unknown":
                        print(f"WARNING: User selected {bill_type} but detected {local_detection}")
                    
                    print(f"Detected: {detected_supplier} - {local_detection}")
        except Exception as e:
            print(f"Local parsing failed: {e}")
        
        # DocuPipe API calls with better error handling
        encoded = base64.b64encode(content).decode()
        
        print("Uploading to DocuPipe...")
        upload_res = requests.post(
            "https://app.docupipe.ai/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPIPE_API_KEY,
            },
            timeout=30  # Add timeout
        )
        
        print(f"DocuPipe upload response: {upload_res.status_code}")
        
        if not upload_res.ok:
            error_msg = f"DocuPipe upload failed: {upload_res.status_code}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        
        if not document_id or not job_id:
            error_msg = f"Missing documentId or jobId in response: {data}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        print(f"Document uploaded: {document_id}, Job: {job_id}")
        
        # Wait for document processing with better error handling
        for attempt in range(10):
            time.sleep(5)
            try:
                res = requests.get(
                    f"https://app.docupipe.ai/job/{job_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                    timeout=10
                )
                if not res.ok:
                    print(f"Job status check failed: {res.status_code}")
                    continue
                    
                status = res.json().get("status")
                print(f"Document processing {attempt + 1}: {status}")
                
                if status == "completed":
                    break
                elif status == "error":
                    error_msg = "Document processing failed"
                    print(error_msg)
                    send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                    return
            except requests.RequestException as e:
                print(f"Request error during job status check: {e}")
                continue
        else:
            error_msg = "Document processing timeout"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        # Start standardization
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS
        print(f"Using schema: {schema_id} for {bill_type}")
        
        try:
            std_res = requests.post(
                "https://app.docupipe.ai/v2/standardize/batch",
                json={"documentIds": [document_id], "schemaId": schema_id},
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                    "X-API-Key": DOCUPIPE_API_KEY,
                },
                timeout=30
            )
            
            if not std_res.ok:
                error_msg = f"Standardization request failed: {std_res.status_code}"
                print(error_msg)
                send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                return
                
            print(f"Standardization started: {std_res.status_code}")
            
        except requests.RequestException as e:
            error_msg = f"Standardization request error: {e}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        std_data = std_res.json()
        std_job_id = std_data.get("jobId")
        std_id = std_data.get("standardizationIds", [None])[0]
        
        if not std_job_id or not std_id:
            error_msg = f"Missing standardization IDs: {std_data}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        # Wait for standardization with better error handling
        print("Waiting for standardization...")
        time.sleep(15)
        
        for attempt in range(12):
            try:
                std_job_res = requests.get(
                    f"https://app.docupipe.ai/job/{std_job_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                    timeout=10
                )
                
                if not std_job_res.ok:
                    print(f"Standardization status check failed: {std_job_res.status_code}")
                    time.sleep(10)
                    continue
                
                status = std_job_res.json().get("status")
                print(f"Standardization {attempt + 1}: {status}")
                
                if status == "completed":
                    # Add delay before fetching result to ensure it's ready
                    print(f"Standardization completed, waiting 5 seconds before fetching result...")
                    time.sleep(5)
                    
                    # Get final result - using correct endpoint from DocuPipe docs
                    print(f"Fetching result from: https://app.docupipe.ai/standardization/{std_id}")
                    result_res = requests.get(
                        f"https://app.docupipe.ai/standardization/{std_id}",  # Fixed endpoint URL
                        headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                        timeout=10
                    )
                    
                    if not result_res.ok:
                        error_msg = f"Failed to get standardization result: {result_res.status_code}"
                        print(error_msg)
                        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                        return
                    
                    result_json = result_res.json()
                    print(f"DocuPipe result keys: {list(result_json.keys())}")
                    
                    # According to DocuPipe docs, the data is in the "data" field
                    parsed = result_json.get("data", {})
                    
                    if not parsed:
                        # Fallback to other possible field names
                        parsed = result_json.get("result", {})
                        if not parsed:
                            error_msg = f"Empty result from DocuPipe. Response: {result_json}"
                            print(error_msg)
                            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                            return
                    
                    print(f"Got parsed data with keys: {list(parsed.keys())}")
                    
                    # Extract billing period
                    billing_start = (
                        parsed.get("billingPeriod", {}).get("startDate") or 
                        parsed.get("billingPeriodStartDate") or 
                        bill_date
                    )
                    
                    # Save to S3 and database
                    try:
                        s3_json_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
                        save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                        save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_json_path)
                        
                        print(f"Successfully saved: {s3_json_path}")
                        
                        # Send success webhook
                        send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_json_path, detected_supplier, "success")
                        return
                        
                    except Exception as e:
                        error_msg = f"Failed to save data: {e}"
                        print(error_msg)
                        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                        return
                    
                elif status == "error":
                    error_msg = "Standardization processing failed"
                    print(error_msg)
                    send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                    return
                else:
                    time.sleep(10)
                    
            except requests.RequestException as e:
                print(f"Request error during standardization check: {e}")
                time.sleep(10)
                continue
        
        error_msg = "Standardization timeout"
        print(error_msg)
        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
        
    except Exception as e:
        error_msg = f"Processing error: {e}"
        print(error_msg)
        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", "Unknown", "error", error_msg)
        import traceback
        traceback.print_exc()
