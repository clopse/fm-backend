from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
import base64
import requests
import os
import time
import pdfplumber
import calendar
from io import BytesIO
from app.db.session import get_db, engine
from app.utils.s3_utils import save_parsed_data_to_s3, get_utility_data_for_hotel_year, get_utility_summary_for_comparison  # NEW
from app.utils.s3 import save_pdf_to_s3
from app.s3_config import s3  # Import your S3 client

router = APIRouter()

DOCUPIPE_API_KEY = os.getenv("DOCUPIPE_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"  # Fixed: was 33093b44, now 33093b4d
UPLOAD_WEBHOOK_URL = os.getenv("UPLOAD_WEBHOOK_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "jmk-project-uploads")  # Add your bucket name

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
        if "flogas" in supplier_lower or "fgnc" in supplier_lower or "gas" in supplier_lower:
            print("Supplier-based detection: GAS (Flogas)")
            return "gas"
        elif "arden" in supplier_lower or "aes" in supplier_lower:
            print("Supplier-based detection: ELECTRICITY (Arden)")
            return "electricity"
        else:
            print("Could not determine bill type - defaulting to electricity")
            return "unknown"  # Default fallback


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


# NEW ENDPOINTS FOR UTILITIES ADMIN PAGE

def get_hotel_ids_from_s3():
    """Get list of hotel IDs by scanning S3 bucket structure"""
    try:
        response = s3.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Delimiter='/'
        )
        
        hotel_ids = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                # Extract hotel ID from prefix (e.g., "hiex/" -> "hiex")
                hotel_id = prefix['Prefix'].rstrip('/')
                hotel_ids.append(hotel_id)
        
        return hotel_ids
    except Exception as e:
        print(f"Error getting hotel IDs from S3: {e}")
        return []


@router.get("/utilities/missing-bills")
async def get_missing_bills():
    """Get missing and overdue utility bills dashboard"""
    try:
        # This is a simple implementation - you can enhance based on your needs
        # You might want to track expected bills in a database or config
        
        current_date = datetime.now()
        missing_bills = []
        
        # Define expected utility types per hotel (you can move this to config)
        # Using your hotel IDs from hotels.ts
        hotel_utilities = {
            "hiex": ["electricity", "gas"],
            "moxy": ["electricity", "gas"], 
            "hida": ["electricity", "gas"],
            "hbhdcc": ["electricity", "gas"],
            "hbhe": ["electricity"],
            "sera": ["electricity", "gas"],
            "marina": ["electricity", "gas"],
            "belfast": ["electricity", "gas"],
            "hiltonth": ["electricity", "gas"]
        }
        
        # Check last 3 months for missing bills
        for months_back in range(1, 4):
            check_date = current_date - timedelta(days=30 * months_back)
            year_month = check_date.strftime("%Y-%m")
            
            for hotel_id, utilities in hotel_utilities.items():
                for utility_type in utilities:
                    # Check if bill exists for this hotel/utility/month
                    try:
                        bills = get_utility_data_for_hotel_year(hotel_id, str(check_date.year))
                        
                        # Check if bill exists for this month
                        bill_exists = any(
                            bill.get("summary", {}).get("bill_date", "").startswith(year_month)
                            for bill in bills 
                            if bill.get("utility_type") == utility_type
                        )
                        
                        if not bill_exists:
                            # Calculate days overdue (assuming bills due by 15th of following month)
                            expected_due = datetime(check_date.year, check_date.month, 15)
                            if check_date.month == 12:
                                expected_due = datetime(check_date.year + 1, 1, 15)
                            else:
                                expected_due = datetime(check_date.year, check_date.month + 1, 15)
                            
                            days_overdue = max(0, (current_date - expected_due).days)
                            
                            if days_overdue > 0:  # Only include if actually overdue
                                missing_bills.append({
                                    "hotel_id": hotel_id,
                                    "utility_type": utility_type,
                                    "expected_month": calendar.month_name[check_date.month],
                                    "expected_year": check_date.year,
                                    "days_overdue": days_overdue,
                                    "last_uploaded": None,  # You could enhance this
                                    "status": "missing" if days_overdue > 30 else "overdue",
                                    "expected_date": expected_due.isoformat(),
                                    "manager_email": f"manager@{hotel_id}.com"  # You can customize this
                                })
                    except Exception as e:
                        print(f"Error checking {hotel_id} {utility_type} for {year_month}: {e}")
                        continue
        
        # Calculate stats
        total_expected = len(hotel_utilities) * 2 * 3  # Rough estimate
        total_missing = len([b for b in missing_bills if b["status"] == "missing"])
        total_overdue = len([b for b in missing_bills if b["status"] == "overdue"])
        compliance_rate = max(0, (total_expected - len(missing_bills)) / total_expected * 100) if total_expected > 0 else 100
        hotels_with_issues = len(set(b["hotel_id"] for b in missing_bills))
        
        stats = {
            "total_expected": total_expected,
            "total_missing": total_missing,
            "total_overdue": total_overdue,
            "compliance_rate": compliance_rate,
            "hotels_with_issues": hotels_with_issues
        }
        
        return {
            "missing_bills": missing_bills,
            "stats": stats
        }
        
    except Exception as e:
        print(f"Error getting missing bills: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get missing bills: {str(e)}")


@router.post("/utilities/send-reminder")
async def send_reminder_email(reminder_data: dict):
    """Send reminder email for missing utility bill"""
    try:
        hotel_id = reminder_data.get("hotel_id")
        utility_type = reminder_data.get("utility_type") 
        month = reminder_data.get("month")
        
        # Here you would integrate with your email system
        # For now, just return success
        print(f"Sending reminder: {hotel_id} - {utility_type} - {month}")
        
        # You can integrate with your existing email system here
        # email_service.send_reminder_email(hotel_id, utility_type, month)
        
        return {"success": True, "message": "Reminder email sent"}
        
    except Exception as e:
        print(f"Error sending reminder: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send reminder: {str(e)}")


@router.get("/utilities/all-bills")
async def get_all_bills():
    """Get all uploaded utility bills for admin viewer"""
    try:
        all_bills = []
        
        # You'll need to implement this based on your S3 structure
        # This is a simplified version - you might want to cache this data
        
        # Get hotel IDs dynamically from S3 bucket structure
        hotel_ids = get_hotel_ids_from_s3()
        current_year = datetime.now().year
        
        if not hotel_ids:
            # Fallback if S3 scan fails
            return {
                "missing_bills": [],
                "stats": {
                    "total_expected": 0,
                    "total_missing": 0,
                    "total_overdue": 0,
                    "compliance_rate": 100,
                    "hotels_with_issues": 0
                }
            }
        
        for hotel_id in hotel_ids:
            try:
                # Get bills for current and previous year
                for year in [current_year, current_year - 1]:
                    bills = get_utility_data_for_hotel_year(hotel_id, str(year))
                    
                    for bill in bills:
                        summary = bill.get("summary", {})
                        
                        all_bills.append({
                            "id": f"{hotel_id}_{bill.get('utility_type')}_{summary.get('bill_date', '')}",
                            "hotel_id": hotel_id,
                            "utility_type": bill.get("utility_type"),
                            "filename": bill.get("filename", "Unknown"),
                            "upload_date": bill.get("upload_date", ""),
                            "bill_period": summary.get("bill_date", ""),
                            "supplier": summary.get("supplier", "Unknown"),
                            "total_amount": summary.get("total_cost", 0) or 0,
                            "consumption": summary.get("total_kwh", 0) or summary.get("consumption_kwh", 0) or 0,
                            "consumption_unit": "kWh",
                            "pdf_url": bill.get("pdf_url"),
                            "parsed_status": "success"  # Since these are successfully parsed bills
                        })
                        
            except Exception as e:
                print(f"Error getting bills for {hotel_id}: {e}")
                continue
        
        # Sort by upload date (newest first)
        all_bills.sort(key=lambda x: x.get("upload_date", ""), reverse=True)
        
        return {"bills": all_bills}
        
    except Exception as e:
        print(f"Error getting all bills: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get all bills: {str(e)}")


@router.get("/utilities/bill-details/{bill_id}")
async def get_bill_details(bill_id: str):
    """Get detailed parsed data for a specific bill"""
    try:
        # Parse bill_id to extract hotel_id, utility_type, and date
        parts = bill_id.split("_")
        if len(parts) < 3:
            raise HTTPException(status_code=400, detail="Invalid bill ID format")
        
        hotel_id = parts[0]
        utility_type = parts[1]
        bill_date = "_".join(parts[2:])  # In case date has underscores
        
        # Get the specific bill data from S3
        year = bill_date[:4] if len(bill_date) >= 4 else str(datetime.now().year)
        bills = get_utility_data_for_hotel_year(hotel_id, year)
        
        # Find the specific bill
        target_bill = None
        for bill in bills:
            if (bill.get("utility_type") == utility_type and 
                bill.get("summary", {}).get("bill_date", "").startswith(bill_date[:7])):
                target_bill = bill
                break
        
        if not target_bill:
            raise HTTPException(status_code=404, detail="Bill not found")
        
        return {
            "bill_id": bill_id,
            "parsed_data": target_bill.get("raw_data", target_bill),  # Return full parsed data
            "summary": target_bill.get("summary", {})
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting bill details: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get bill details: {str(e)}")


@router.get("/utilities/download-pdf/{bill_id}")
async def download_bill_pdf(bill_id: str):
    """Download PDF file for a specific bill"""
    try:
        # This would need to be implemented based on your S3 PDF storage structure
        # For now, return a redirect or signed URL
        
        parts = bill_id.split("_")
        if len(parts) < 3:
            raise HTTPException(status_code=400, detail="Invalid bill ID format")
        
        hotel_id = parts[0]
        utility_type = parts[1] 
        bill_date = "_".join(parts[2:])
        
        # You would generate a signed S3 URL here
        # pdf_url = generate_s3_signed_url(hotel_id, utility_type, bill_date)
        
        # For now, return info about where the PDF would be
        return {
            "message": "PDF download would be implemented here",
            "hotel_id": hotel_id,
            "utility_type": utility_type,
            "bill_date": bill_date
        }
        
    except Exception as e:
        print(f"Error downloading PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to download PDF: {str(e)}")


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
