from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks, HTTPException
from datetime import datetime, timedelta
import base64
import requests
import os
import time
import pdfplumber
import calendar
import json
import boto3
from io import BytesIO, StringIO
import csv
from fastapi.responses import StreamingResponse, Response
from app.utils.s3 import save_pdf_to_s3

router = APIRouter()

DOCUPIPE_API_KEY = os.getenv("DOCUPIPE_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"
UPLOAD_WEBHOOK_URL = os.getenv("UPLOAD_WEBHOOK_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "jmk-project-uploads")

# Initialize S3 client
s3_client = boto3.client('s3')

@router.\1("\2")
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
    
    gas_keywords = ["mprn", "gprn", "gas usage", "therms", "cubic feet", "calorific value", "gas supply", "natural gas", "flogas", "fgnc"]
    electricity_keywords = ["mpan", "kwh", "kilowatt", "day units", "night units", "electricity", "electric", "arden", "aes916", "mic", "maximum import capacity"]
    
    gas_matches = sum(1 for keyword in gas_keywords if keyword in full_text)
    electricity_matches = sum(1 for keyword in electricity_keywords if keyword in full_text)
    
    if gas_matches > electricity_matches:
        return "gas"
    elif electricity_matches > gas_matches:
        return "electricity"
    else:
        supplier_lower = supplier.lower()
        if "flogas" in supplier_lower or "fgnc" in supplier_lower or "gas" in supplier_lower:
            return "gas"
        elif "arden" in supplier_lower or "aes" in supplier_lower:
            return "electricity"
        else:
            return "unknown"

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

@router.\1("\2")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    file: UploadFile = File(...),
    bill_date: str = Form(...),
    bill_type: str = Form(...),
    supplier: str = Form(default="docupanda")
):
    if not DOCUPIPE_API_KEY:
        raise HTTPException(status_code=500, detail="DocuPipe API key not configured")
    
    if bill_type not in ["electricity", "gas"]:
        raise HTTPException(status_code=400, detail="Invalid bill type. Must be 'electricity' or 'gas'")
    
    content = await file.read()
    filename = file.filename
    
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    
    if not filename or not filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    background_tasks.add_task(
        process_and_store_docupipe, 
        content, hotel_id, filename, bill_date, bill_type, supplier
    )
    
    return {"status": "processing", "message": "Upload received. Processing in background."}

def process_and_store_docupipe(content, hotel_id, filename, bill_date, bill_type, supplier="docupanda"):
    try:
        print(f"Processing {filename} - Type: {bill_type}, Hotel: {hotel_id}")
        
        detected_supplier = "Unknown"
        detected_bill_type = bill_type
        
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
                    
                    if local_detection != bill_type and local_detection != "unknown":
                        print(f"WARNING: User selected {bill_type} but detected {local_detection}")
                    
                    print(f"Detected: {detected_supplier} - {local_detection}")
        except Exception as e:
            print(f"Local parsing failed: {e}")
        
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
            timeout=30
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
            
            print(f"v2/standardize/batch response: {std_res.status_code}")
            
            if not std_res.ok:
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
            
            if not std_res.ok:
                error_msg = f"Both standardization endpoints failed: {std_res.status_code}"
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
                    print(f"Standardization completed, waiting 5 seconds before fetching result...")
                    time.sleep(5)
                    
                    print(f"Fetching result from: https://app.docupipe.ai/standardization/{std_id}")
                    result_res = requests.get(
                        f"https://app.docupipe.ai/standardization/{std_id}",
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
                    
                    parsed = result_json.get("data", {})
                    
                    if not parsed:
                        parsed = result_json.get("result", {})
                        if not parsed:
                            error_msg = f"Empty result from DocuPipe. Response: {result_json}"
                            print(error_msg)
                            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                            return
                    
                    print(f"Got parsed data with keys: {list(parsed.keys())}")
                    
                    billing_start = (
                        parsed.get("billingPeriod", {}).get("startDate") or 
                        parsed.get("billingPeriodStartDate") or 
                        bill_date
                    )
                    
                    try:
                        s3_json_path = save_parsed_data_to_s3(hotel_id, bill_type, parsed, "", filename)
                        save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                        
                        print(f"Successfully saved: {s3_json_path}")
                        
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

def save_parsed_data_to_s3(hotel_id: str, bill_type: str, parsed_data: dict, upload_date: str, filename: str):
    """Save parsed utility bill data to S3 with proper structure"""
    try:
        summary = extract_bill_summary_from_real_data(parsed_data, bill_type)
        
        bill_data = {
            "hotel_id": hotel_id,
            "utility_type": bill_type,
            "filename": filename,
            "uploaded_at": upload_date or datetime.utcnow().isoformat(),
            "summary": summary,
            "raw_data": parsed_data
        }
        
        bill_date = summary.get('bill_date', '')
        if bill_date:
            year = bill_date.split('-')[0]
            month = bill_date.split('-')[1] if len(bill_date.split('-')) > 1 else '01'
        else:
            now = datetime.utcnow()
            year = now.strftime('%Y')
            month = now.strftime('%m')
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        base_filename = filename.replace('.pdf', '').replace('.json', '')
        s3_key = f"utilities/{hotel_id}/{year}/{month}/{bill_type}_{base_filename}_{timestamp}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(bill_data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        print(f"Successfully saved bill to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        print(f"Error saving to S3: {e}")
        raise e

def extract_bill_summary_from_real_data(data: dict, bill_type: str) -> dict:
    """Extract summary from real parsed bill data structures"""
    summary = {}
    
    try:
        if bill_type == 'electricity':
            summary.update({
                'supplier': data.get('supplier', 'Unknown'),
                'customer_ref': data.get('customerRef', ''),
                'billing_ref': data.get('billingRef', ''),
                'account_number': data.get('customerRef', ''),
                'meter_number': data.get('meterDetails', {}).get('meterNumber', ''),
                'mprn': data.get('meterDetails', {}).get('mprn', ''),
                'bill_date': data.get('billingPeriod', {}).get('endDate', ''),
                'billing_period_start': data.get('billingPeriod', {}).get('startDate', ''),
                'billing_period_end': data.get('billingPeriod', {}).get('endDate', ''),
                'total_cost': data.get('totalAmount', {}).get('value', 0),
                'mic_value': data.get('meterDetails', {}).get('mic', {}).get('value', 0),
                'max_demand': data.get('meterDetails', {}).get('maxDemand', {}).get('value', 0),
                'vat_amount': data.get('taxDetails', {}).get('vatAmount', 0),
                'electricity_tax': data.get('taxDetails', {}).get('electricityTax', {}).get('amount', 0)
            })
            
            consumption = data.get('consumption', [])
            day_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Day'), 0)
            night_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Night'), 0)
            
            summary.update({
                'day_kwh': day_kwh,
                'night_kwh': night_kwh,
                'total_kwh': day_kwh + night_kwh
            })
            
        elif bill_type == 'gas':
            summary.update({
                'supplier': data.get('supplierInfo', {}).get('name', 'Unknown'),
                'account_number': data.get('accountInfo', {}).get('accountNumber', ''),
                'gprn': data.get('accountInfo', {}).get('gprn', ''),
                'meter_number': data.get('accountInfo', {}).get('meterNumber', ''),
                'bill_date': data.get('billSummary', {}).get('billingPeriodEndDate', ''),
                'billing_period_start': data.get('billSummary', {}).get('billingPeriodStartDate', ''),
                'billing_period_end': data.get('billSummary', {}).get('billingPeriodEndDate', ''),
                'total_cost': data.get('billSummary', {}).get('currentBillAmount', 0),
                'consumption_kwh': data.get('consumptionDetails', {}).get('consumptionValue', 0),
                'units_consumed': data.get('meterReadings', {}).get('unitsConsumed', 0),
                'conversion_factor': data.get('consumptionDetails', {}).get('conversionFactor', 0),
                'vat_amount': data.get('billSummary', {}).get('totalVatAmount', 0)
            })
            
            line_items = data.get('lineItems', [])
            carbon_tax = next((item.get('amount', 0) for item in line_items if 'carbon' in item.get('description', '').lower()), 0)
            standing_charge = next((item.get('amount', 0) for item in line_items if 'standing' in item.get('description', '').lower()), 0)
            commodity_cost = next((item.get('amount', 0) for item in line_items if 'commodity' in item.get('description', '').lower() or 'tariff' in item.get('description', '').lower()), 0)
            
            summary.update({
                'carbon_tax': carbon_tax,
                'standing_charge': standing_charge,
                'commodity_cost': commodity_cost
            })
    
    except Exception as e:
        print(f"Error extracting summary for {bill_type}: {e}")
    
    return summary

def get_utility_data_for_hotel_year(hotel_id: str, year: str):
    """Get utility bills data from S3 - CORRECTED for your actual S3 structure"""
    try:
        bills = []
        json_prefix = f"utilities/{hotel_id}/{year}/"
        
        print(f"Searching for {hotel_id} {year} bills in S3 at: {json_prefix}")
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=json_prefix
            )
            
            if 'Contents' in response:
                print(f"Found {len(response['Contents'])} objects in {json_prefix}")
                
                for obj in response['Contents']:
                    key = obj['Key']
                    print(f"Processing file: {key}")
                    
                    if not key.endswith('.json'):
                        print(f"Skipping non-JSON file: {key}")
                        continue
                    
                    try:
                        file_response = s3_client.get_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=key
                        )
                        
                        bill_data = json.loads(file_response['Body'].read())
                        
                        if not isinstance(bill_data, dict):
                            print(f"Skipping invalid JSON structure: {key}")
                            continue
                        
                        bill_date = None
                        if 'summary' in bill_data:
                            bill_date = bill_data['summary'].get('bill_date', '')
                        elif 'raw_data' in bill_data:
                            raw_data = bill_data['raw_data']
                            bill_date = (
                                raw_data.get('billingPeriod', {}).get('endDate', '') or
                                raw_data.get('billSummary', {}).get('billingPeriodEndDate', '')
                            )
                        
                        if bill_date and not bill_date.startswith(year):
                            print(f"Skipping bill from wrong year: {bill_date} (want {year})")
                            continue
                        
                        if not bill_data.get('utility_type'):
                            raw_data = bill_data.get('raw_data', {})
                            if 'supplier' in raw_data and 'arden' in raw_data.get('supplier', '').lower():
                                bill_data['utility_type'] = 'electricity'
                            elif 'supplierInfo' in raw_data or 'documentType' in raw_data:
                                bill_data['utility_type'] = 'gas'
                            else:
                                print(f"Skipping bill with unknown utility type: {key}")
                                continue
                        
                        print(f"✅ Adding bill: {key} for date {bill_date}, type: {bill_data.get('utility_type')}")
                        bills.append(bill_data)
                        
                    except json.JSONDecodeError as e:
                        print(f"JSON parse error for {key}: {e}")
                        continue
                    except Exception as e:
                        print(f"Error processing {key}: {e}")
                        continue
            else:
                print(f"No objects found in {json_prefix}")
                
        except Exception as e:
            print(f"Error checking S3 prefix {json_prefix}: {e}")
        
        print(f"Total bills found for {hotel_id} {year}: {len(bills)}")
        return bills
        
    except Exception as e:
        print(f"Error in get_utility_data_for_hotel_year: {e}")
        return []

@router.\1("\2")
async def get_bill_pdf_direct(hotel_id: str, utility_type: str, year: str, filename: str):
    """Download PDF for a specific bill using direct S3 path"""
    try:
        # Construct S3 key directly from path parameters
        s3_key = f"{hotel_id}/{utility_type}/{year}/{filename}"
        
        print(f"Looking for PDF at S3 key: {s3_key}")
        
        try:
            # Get PDF from S3
            pdf_response = s3_client.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key
            )
            
            pdf_content = pdf_response['Body'].read()
            
            # Return PDF with proper headers
            return Response(
                content=pdf_content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}",
                    "Content-Length": str(len(pdf_content))
                }
            )
            
        except s3_client.exceptions.NoSuchKey:
            raise HTTPException(status_code=404, detail=f"PDF file not found at {s3_key}")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting bill PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get bill PDF: {str(e)}")
@router.\1("\2")
async def get_bill_pdf(bill_id: str):
    """Download PDF for a specific bill"""
    try:
        # Parse bill_id to extract hotel_id, utility_type, and date
        parts = bill_id.split("_")
        if len(parts) < 3:
            raise HTTPException(status_code=400, detail="Invalid bill ID format")
        
        hotel_id = parts[0]
        utility_type = parts[1]
        bill_date = "_".join(parts[2:])  # In case date has underscores
        
        # Get the specific bill data from S3 JSON to find PDF location
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
        
        # Get the original filename to construct PDF path
        original_filename = target_bill.get("filename", "")
        if not original_filename:
            raise HTTPException(status_code=404, detail="Original filename not found")
        
        # Construct PDF S3 key based on your storage pattern
        # PDFs are stored at: {hotel_id}/electricity/{year}/filename.pdf
        billing_start = target_bill.get("summary", {}).get("billing_period_start", "")
        billing_year = billing_start[:4] if billing_start else year
        
        # Your PDF storage pattern
        pdf_key = f"{hotel_id}/{utility_type}/{billing_year}/{original_filename}"
        
        print(f"Looking for PDF at S3 key: {pdf_key}")
        
        try:
            # Get PDF from S3
            pdf_response = s3_client.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=pdf_key
            )
            
            pdf_content = pdf_response['Body'].read()
            
            # Generate appropriate filename based on bill period
            summary = target_bill.get("summary", {})
            supplier = summary.get("supplier", "Unknown").replace(" ", "")
            
            start_date = summary.get("billing_period_start")
            end_date = summary.get("billing_period_end") or summary.get("bill_date")
            
            if start_date and end_date:
                # Format dates for filename (e.g., GAS_Flogas_Dec24-Jan25.pdf)
                start = datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.strptime(end_date, '%Y-%m-%d')
                
                start_formatted = start.strftime('%b%y')
                end_formatted = end.strftime('%b%y')
                
                if start_formatted == end_formatted:
                    filename = f"{utility_type.upper()}_{supplier}_{start_formatted}.pdf"
                else:
                    filename = f"{utility_type.upper()}_{supplier}_{start_formatted}-{end_formatted}.pdf"
            else:
                # Fallback filename
                filename = f"{utility_type.upper()}_{supplier}_bill.pdf"
            
            # Return PDF with proper headers
            return Response(
                content=pdf_content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}",
                    "Content-Length": str(len(pdf_content))
                }
            )
            
        except s3_client.exceptions.NoSuchKey:
            # Try alternative PDF locations if not found
            alternative_keys = [
                f"{hotel_id}/{utility_type}/{year}/{original_filename}",
                f"pdfs/{hotel_id}/{utility_type}/{year}/{original_filename}",
                f"bills/{hotel_id}/{utility_type}/{original_filename}",
                f"{hotel_id}/bills/{utility_type}/{original_filename}"
            ]
            
            for alt_key in alternative_keys:
                try:
                    print(f"Trying alternative PDF location: {alt_key}")
                    pdf_response = s3_client.get_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=alt_key
                    )
                    
                    pdf_content = pdf_response['Body'].read()
                    
                    # Use same filename generation as above
                    summary = target_bill.get("summary", {})
                    supplier = summary.get("supplier", "Unknown").replace(" ", "")
                    start_date = summary.get("billing_period_start")
                    end_date = summary.get("billing_period_end") or summary.get("bill_date")
                    
                    if start_date and end_date:
                        start = datetime.strptime(start_date, '%Y-%m-%d')
                        end = datetime.strptime(end_date, '%Y-%m-%d')
                        start_formatted = start.strftime('%b%y')
                        end_formatted = end.strftime('%b%y')
                        
                        if start_formatted == end_formatted:
                            filename = f"{utility_type.upper()}_{supplier}_{start_formatted}.pdf"
                        else:
                            filename = f"{utility_type.upper()}_{supplier}_{start_formatted}-{end_formatted}.pdf"
                    else:
                        filename = f"{utility_type.upper()}_{supplier}_bill.pdf"
                    
                    return Response(
                        content=pdf_content,
                        media_type="application/pdf",
                        headers={
                            "Content-Disposition": f"attachment; filename={filename}",
                            "Content-Length": str(len(pdf_content))
                        }
                    )
                    
                except s3_client.exceptions.NoSuchKey:
                    continue
            
            # If no PDF found in any location
            raise HTTPException(status_code=404, detail=f"PDF file not found for bill {bill_id}")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting bill PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get bill PDF: {str(e)}")

@router.get("/{hotel_id}/bills")
async def get_raw_bills_data(hotel_id: str, year: str = None):
    """Get raw bill data with full DocuPipe JSON for advanced filtering"""
    try:
        current_year = datetime.now().year
        years_to_check = [year] if year else [str(current_year), str(current_year - 1)]
        
        all_bills = []
        
        for check_year in years_to_check:
            bills = get_utility_data_for_hotel_year(hotel_id, check_year)
            
            for bill in bills:
                try:
                    if not isinstance(bill, dict):
                        continue
                    
                    summary = bill.get('summary', {})
                    raw_data = bill.get('raw_data', bill)
                    
                    utility_type = bill.get('utility_type')
                    if not utility_type:
                        if 'supplier' in raw_data and 'arden' in raw_data.get('supplier', '').lower():
                            utility_type = 'electricity'
                        elif 'supplierInfo' in raw_data or 'documentType' in raw_data:
                            utility_type = 'gas'
                        else:
                            utility_type = 'unknown'
                    
                    bill_date = (
                        summary.get('bill_date') or
                        raw_data.get('billingPeriod', {}).get('endDate') or
                        raw_data.get('billSummary', {}).get('billingPeriodEndDate') or
                        ''
                    )
                    
                    enhanced_bill = {
                        **bill,
                        'utility_type': utility_type,
                        'enhanced_summary': {
                            **summary,
                            'bill_date': bill_date,
                            'month_year': bill_date[:7] if bill_date else '',
                            'year': bill_date[:4] if bill_date else ''
                        }
                    }
                    
                    all_bills.append(enhanced_bill)
                    
                except Exception as e:
                    print(f"Error processing bill: {e}")
                    continue
        
        all_bills.sort(
            key=lambda x: x.get('enhanced_summary', {}).get('bill_date', ''), 
            reverse=True
        )
        
        return {
            "hotel_id": hotel_id,
            "year": year or "all",
            "bills": all_bills,
            "total_bills": len(all_bills),
            "bills_by_type": {
                "electricity": len([b for b in all_bills if b.get('utility_type') == 'electricity']),
                "gas": len([b for b in all_bills if b.get('utility_type') == 'gas']),
                "water": len([b for b in all_bills if b.get('utility_type') == 'water']),
                "unknown": len([b for b in all_bills if b.get('utility_type') == 'unknown'])
            }
        }
        
    except Exception as e:
        print(f"Error fetching raw bills: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch bills: {str(e)}")

@router.\1("\2")
async def get_utilities_data(hotel_id: str, year: int):
    """Get utility bills data from S3 for charts - S3 ONLY VERSION"""
    try:
        bills = get_utility_data_for_hotel_year(hotel_id, str(year))
        
        electricity_data = []
        gas_data = []
        water_data = []
        
        processed_count = {"electricity": 0, "gas": 0, "water": 0}
        months_seen = {"electricity": set(), "gas": set()}
        
        for bill in bills:
            try:
                utility_type = bill.get('utility_type')
                if not utility_type:
                    raw_data = bill.get('raw_data', bill)
                    if 'supplier' in raw_data and 'arden' in raw_data.get('supplier', '').lower():
                        utility_type = 'electricity'
                    elif 'supplierInfo' in raw_data or 'documentType' in raw_data:
                        utility_type = 'gas'
                    else:
                        continue
                
                summary = bill.get('summary', {})
                raw_data = bill.get('raw_data', bill)
                
                bill_date = (
                    summary.get('bill_date') or
                    summary.get('billing_period_end') or
                    raw_data.get('billingPeriod', {}).get('endDate') or
                    raw_data.get('billSummary', {}).get('billingPeriodEndDate') or
                    ''
                )
                
                if not bill_date.startswith(str(year)):
                    continue
                
                month_key = bill_date[:7]
                
                if utility_type in months_seen and month_key in months_seen[utility_type]:
                    print(f"WARNING: Duplicate month detected at endpoint level: {utility_type} {month_key}")
                    continue
                
                months_seen[utility_type].add(month_key)
                
                if utility_type == 'electricity':
                    consumption = raw_data.get('consumption', [])
                    day_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Day'), 0)
                    night_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Night'), 0)
                    total_kwh = day_kwh + night_kwh
                    total_cost = raw_data.get('totalAmount', {}).get('value', 0)
                    
                    electricity_data.append({
                        "month": month_key,
                        "day_kwh": day_kwh,
                        "night_kwh": night_kwh,
                        "total_kwh": total_kwh,
                        "total_eur": total_cost,
                        "per_room_kwh": total_kwh / 100,
                        "bill_id": bill.get('filename', f'electricity_{month_key}')
                    })
                    processed_count["electricity"] += 1
                
                elif utility_type == 'gas':
                    consumption_kwh = raw_data.get('consumptionDetails', {}).get('consumptionValue', 0)
                    total_cost = raw_data.get('billSummary', {}).get('currentBillAmount', 0)
                    
                    gas_data.append({
                        "period": month_key,
                        "total_kwh": consumption_kwh,
                        "total_eur": total_cost,
                        "per_room_kwh": consumption_kwh / 100,
                        "bill_id": bill.get('filename', f'gas_{month_key}')
                    })
                    processed_count["gas"] += 1
                
            except Exception as e:
                print(f"Error processing individual bill: {e}")
                continue
        
        electricity_total = sum(item.get('total_kwh', 0) for item in electricity_data)
        gas_total = sum(item.get('total_kwh', 0) for item in gas_data)
        electricity_cost_total = sum(item.get('total_eur', 0) for item in electricity_data)
        gas_cost_total = sum(item.get('total_eur', 0) for item in gas_data)
        
        return {
            "electricity": electricity_data,
            "gas": gas_data,
            "water": water_data,
            "totals": {
                "electricity": electricity_total,
                "gas": gas_total,
                "electricity_cost": electricity_cost_total,
                "gas_cost": gas_cost_total
            },
            "processed_counts": processed_count,
            "total_bills_found": len(bills),
            "debug_info": {
                "months_processed": {
                    "electricity": list(months_seen['electricity']),
                    "gas": list(months_seen['gas'])
                }
            }
        }
        
    except Exception as e:
        print(f"Error fetching utilities data for {hotel_id} {year}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch utilities data: {str(e)}")

@router.\1("\2")
async def debug_actual_structure(hotel_id: str, year: str = "2025"):
    """Debug endpoint to check the actual S3 structure based on your real paths"""
    try:
        results = {
            "hotel_id": hotel_id,
            "year": year,
            "json_location": f"utilities/{hotel_id}/{year}/",
            "findings": {}
        }
        
        json_prefix = f"utilities/{hotel_id}/{year}/"
        try:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=json_prefix,
                MaxKeys=100
            )
            
            json_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.json'):
                        json_files.append({
                            "key": obj['Key'],
                            "size": obj['Size'],
                            "last_modified": obj['LastModified'].isoformat()
                        })
            
            results["findings"]["json_location"] = {
                "prefix": json_prefix,
                "files_found": len(json_files),
                "files": json_files
            }
            
        except Exception as e:
            results["findings"]["json_location"] = {"error": str(e)}
        
        return results
        
    except Exception as e:
        return {"error": str(e)}

@router.\1("\2")
async def debug_test_single_bill(hotel_id: str, year: str = "2025"):
    """Test reading a single bill to verify the process"""
    try:
        bills = get_utility_data_for_hotel_year(hotel_id, year)
        
        if not bills:
            return {
                "error": "No bills found",
                "hotel_id": hotel_id,
                "year": year,
                "search_path": f"utilities/{hotel_id}/{year}/"
            }
        
        first_bill = bills[0]
        
        analysis = {
            "hotel_id": hotel_id,
            "year": year,
            "total_bills_found": len(bills),
            "first_bill_analysis": {
                "filename": first_bill.get('filename'),
                "utility_type": first_bill.get('utility_type'),
                "has_summary": 'summary' in first_bill,
                "has_raw_data": 'raw_data' in first_bill,
                "summary_keys": list(first_bill.get('summary', {}).keys()),
                "raw_data_keys": list(first_bill.get('raw_data', {}).keys())
            }
        }
        
        raw_data = first_bill.get('raw_data', {})
        if first_bill.get('utility_type') == 'electricity':
            consumption = raw_data.get('consumption', [])
            analysis["extraction_test"] = {
                "day_kwh": next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Day'), 0),
                "night_kwh": next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Night'), 0),
                "total_cost": raw_data.get('totalAmount', {}).get('value', 0),
                "bill_date": raw_data.get('billingPeriod', {}).get('endDate', '')
            }
        elif first_bill.get('utility_type') == 'gas':
            analysis["extraction_test"] = {
                "consumption_kwh": raw_data.get('consumptionDetails', {}).get('consumptionValue', 0),
                "total_cost": raw_data.get('billSummary', {}).get('currentBillAmount', 0),
                "bill_date": raw_data.get('billSummary', {}).get('billingPeriodEndDate', '')
            }
        
        return analysis
        
    except Exception as e:
        return {"error": str(e)}



