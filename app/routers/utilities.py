from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
import base64
import requests
import os
import time
import pdfplumber
from io import BytesIO
from app.db.session import get_db, engine
from app.utils.s3_utils import save_parsed_data_to_s3, get_utility_data_for_hotel_year, get_utility_summary_for_comparison  # NEW
from app.utils.s3 import save_pdf_to_s3

router = APIRouter()

DOCUPIPE_API_KEY = os.getenv("DOCUPIPE_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"  # Fixed: was 33093b44, now 33093b4d
UPLOAD_WEBHOOK_URL = os.getenv("UPLOAD_WEBHOOK_URL")

@router.post("/utilities/create-table-simple")
async def create_table_simple():
    """Create table with single SQL statement - for Render deployment"""
    try:
        sql = "CREATE TABLE IF NOT EXISTS utility_bills (id INTEGER PRIMARY KEY AUTOINCREMENT, hotel_id TEXT, gas_supplierInfo_name TEXT, gas_supplierInfo_vatRegNo TEXT, gas_supplierInfo_phoneNumber TEXT, gas_supplierInfo_email TEXT, gas_supplierInfo_address_street TEXT, gas_supplierInfo_address_city TEXT, gas_supplierInfo_address_postalCode TEXT, gas_customerInfo_name TEXT, gas_customerInfo_address_street TEXT, gas_customerInfo_address_city TEXT, gas_customerInfo_address_postalCode TEXT, gas_customerInfo_contactNumber TEXT, gas_accountInfo_accountNumber TEXT, gas_accountInfo_gprn TEXT, gas_accountInfo_meterNumber TEXT, gas_accountInfo_tariffCategory TEXT, gas_accountInfo_paymentMethod TEXT, gas_billSummary_invoiceNumber TEXT, gas_billSummary_issueDate TEXT, gas_billSummary_dueDate TEXT, gas_billSummary_billingPeriodStartDate TEXT, gas_billSummary_billingPeriodEndDate TEXT, gas_billSummary_lastBillAmount REAL, gas_billSummary_paymentReceivedAmount REAL, gas_billSummary_balanceBroughtForward REAL, gas_billSummary_netBillAmount REAL, gas_billSummary_totalVatAmount REAL, gas_billSummary_currentBillAmount REAL, gas_billSummary_totalDueAmount REAL, gas_meterReadings_previousReading REAL, gas_meterReadings_presentReading REAL, gas_meterReadings_unitsConsumed REAL, gas_consumptionDetails_consumptionValue REAL, gas_consumptionDetails_consumptionUnit TEXT, gas_consumptionDetails_calibrationValue REAL, gas_consumptionDetails_conversionFactor REAL, gas_consumptionDetails_correctionFactor REAL, electricity_supplier TEXT, electricity_customerRef TEXT, electricity_billingRef TEXT, electricity_customer_name TEXT, electricity_customer_address_street TEXT, electricity_customer_address_city TEXT, electricity_customer_address_postalCode TEXT, electricity_meterDetails_mprn TEXT, electricity_meterDetails_meterNumber TEXT, electricity_meterDetails_meterType TEXT, electricity_meterDetails_mic_value REAL, electricity_meterDetails_mic_unit TEXT, electricity_meterDetails_maxDemand_value REAL, electricity_meterDetails_maxDemand_unit TEXT, electricity_meterDetails_maxDemandDate TEXT, electricity_consumption_day_kwh REAL, electricity_consumption_night_kwh REAL, electricity_consumption_wattless_kwh REAL, electricity_charge_StandingCharge REAL, electricity_charge_DayUnits REAL, electricity_charge_NightUnits REAL, electricity_charge_LowPowerFactor REAL, electricity_charge_CapacityCharge REAL, electricity_charge_MICExcessCharge REAL, electricity_charge_WinterDemandCharge REAL, electricity_charge_PSOLevy REAL, electricity_charge_ElectricityTax REAL, electricity_taxDetails_vatRate REAL, electricity_taxDetails_vatAmount REAL, electricity_taxDetails_electricityTax_amount REAL, electricity_totalAmount_value REAL, electricity_supplierContact_address TEXT, electricity_supplierContact_phone_1 TEXT, electricity_supplierContact_phone_2 TEXT, electricity_supplierContact_email TEXT, electricity_supplierContact_website TEXT, electricity_supplierContact_vatNumber TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, s3_json_path TEXT)"
        
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        return {"message": "Table created successfully!"}
    except Exception as e:
        return {"error": str(e)}

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
    
    # More comprehensive keywords
    gas_keywords = ["mprn", "gprn", "gas usage", "therms", "cubic feet", "calorific value", "gas supply", "natural gas", "flogas", "fgnc"]
    electricity_keywords = ["mpan", "kwh", "kilowatt", "day units", "night units", "electricity", "electric", "arden", "aes916", "mic", "maximum import capacity"]
    
    gas_matches = sum(1 for keyword in gas_keywords if keyword in full_text)
    electricity_matches = sum(1 for keyword in electricity_keywords if keyword in full_text)
    
    print(f"Gas keywords found: {gas_matches}, Electricity keywords found: {electricity_matches}")
    print(f"Supplier: {supplier}")
    
    # First try keyword matching
    if gas_matches > electricity_matches:
        return "gas"
    elif electricity_matches > gas_matches:
        return "electricity"
    else:
        # Fallback to supplier-based detection when keywords are equal/unclear
        supplier_lower = supplier.lower()
        if "flogas" in supplier_lower or "fgnc" in supplier_lower:
            print("Supplier-based detection: GAS (Flogas)")
            return "gas"
        elif "arden" in supplier_lower or "aes" in supplier_lower:
            print("Supplier-based detection: ELECTRICITY (Arden)")
            return "electricity"
        else:
            print("Could not determine bill type - defaulting to electricity")
            return "electricity"  # Default fallback


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
        
        # Start standardization - try different endpoints
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS
        print(f"Using schema: {schema_id} for {bill_type}")
        
        try:
            # Try the v2 endpoint first
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
            
            print(f"v2/standardize/batch response: {std_res.status_code}")
            print(f"v2/standardize/batch response body: {std_res.text}")
            
            if not std_res.ok:
                # Try the v1 endpoint as fallback
                print("v2 failed, trying v1 endpoint...")
                std_res = requests.post(
                    "https://app.docupipe.ai/standardize/batch",
                    json={"documentIds": [document_id], "schemaId": schema_id},
                    headers={
                        "accept": "application/json",
                        "content-type": "application/json",
                        "X-API-Key": DOCUPIPE_API_KEY,
                    },
                    timeout=30
                )
                print(f"v1/standardize/batch response: {std_res.status_code}")
                print(f"v1/standardize/batch response body: {std_res.text}")
            
            if not std_res.ok:
                error_msg = f"Both standardization endpoints failed: v2={std_res.status_code}, response={std_res.text}"
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
                    
                    # Save to S3 only (no database)
                    try:
                        s3_json_path = save_parsed_data_to_s3(hotel_id, bill_type, parsed, "", filename)
                        save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                        
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


@router.get("/utilities/{hotel_id}/{year}")
async def get_utilities_data(hotel_id: str, year: int):
    """Get utility bills data from S3 for charts"""
    try:
        bills = get_utility_data_for_hotel_year(hotel_id, str(year))
        
        # Format data for frontend charts
        electricity_data = []
        gas_data = []
        
        for bill in bills:
            summary = bill.get("summary", {})
            bill_date = summary.get("bill_date", "")
            
            # FILTER: Only include bills from the requested year
            if not bill_date.startswith(str(year)):
                continue
            
            if bill["utility_type"] == "electricity":
                electricity_data.append({
                    "month": bill_date[:7],  # YYYY-MM
                    "day_kwh": summary.get("day_kwh", 0) or 0,
                    "night_kwh": summary.get("night_kwh", 0) or 0,
                    "total_kwh": summary.get("total_kwh", 0) or 0,
                    "total_eur": summary.get("total_cost", 0) or 0,
                    "per_room_kwh": (summary.get("total_kwh", 0) or 0) / 100  # Adjust room count as needed
                })
            
            elif bill["utility_type"] == "gas":
                gas_data.append({
                    "period": bill_date[:7],  # YYYY-MM
                    "total_kwh": summary.get("consumption_kwh", 0) or 0,
                    "total_eur": summary.get("total_cost", 0) or 0,
                    "per_room_kwh": (summary.get("consumption_kwh", 0) or 0) / 100  # Adjust room count as needed
                })
        
        return {
            "electricity": electricity_data,
            "gas": gas_data,
            "water": []  # Add water data later if needed
        }
        
    except Exception as e:
        print(f"Error fetching utilities data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch utilities data: {str(e)}")


@router.get("/utilities/comparison/{year}")
async def get_utilities_comparison(year: int, hotel_ids: str = "hiex,moxy,hida,hbhdcc,hbhe,sera,marina"):
    """Get utility comparison data for multiple hotels"""
    try:
        hotel_list = hotel_ids.split(",")
        comparison_data = get_utility_summary_for_comparison(hotel_list, str(year))
        
        return {
            "comparison": comparison_data,
            "year": year,
            "hotels": hotel_list
        }
        
    except Exception as e:
        print(f"Error fetching comparison data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch comparison data: {str(e)}")
