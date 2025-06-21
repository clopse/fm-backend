from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks, HTTPException, Request
from datetime import datetime, timedelta
import base64
import requests
import os
import time
import pdfplumber
import calendar
import json
import boto3
import hashlib
from io import BytesIO, StringIO
import csv
from fastapi.responses import StreamingResponse, Response
from app.utils.s3 import save_pdf_to_s3
from typing import List, Dict, Optional, Tuple

# CHANGE: Import SmartBillExtractor instead of DocuPipe
from app.utils.smart_bill_extractor import process_with_smart_bill_extractor

router = APIRouter()

# CHANGE: Replace DocuPipe API key with Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"  # Keep for reference
SCHEMA_GAS = "0804d026"  # Keep for reference
UPLOAD_WEBHOOK_URL = os.getenv("UPLOAD_WEBHOOK_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "jmk-project-uploads")

# Initialize S3 client
s3_client = boto3.client('s3')

# ============= DUPLICATE DETECTION FUNCTIONS (UNCHANGED) =============

def create_bill_fingerprint(bill_data: dict) -> str:
    """Create unique fingerprint for duplicate detection"""
    try:
        summary = bill_data.get('summary', {})
        raw_data = bill_data.get('raw_data', {})
        
        # Get key fields for comparison
        meter_number = (
            summary.get('meter_number', '') or 
            raw_data.get('meterDetails', {}).get('meterNumber', '') or
            raw_data.get('accountInfo', {}).get('meterNumber', '')
        ).strip().lower()
        
        bill_date = (
            summary.get('bill_date', '') or
            raw_data.get('billingPeriod', {}).get('endDate', '') or
            raw_data.get('billSummary', {}).get('billingPeriodEndDate', '')
        )
        
        billing_start = (
            summary.get('billing_period_start', '') or
            raw_data.get('billingPeriod', {}).get('startDate', '') or
            raw_data.get('billSummary', {}).get('billingPeriodStartDate', '')
        )
        
        total_cost = (
            summary.get('total_cost', 0) or
            raw_data.get('totalAmount', {}).get('value', 0) or
            raw_data.get('billSummary', {}).get('currentBillAmount', 0)
        )
        
        # Create fingerprint string
        fingerprint_parts = [
            bill_data.get('hotel_id', ''),
            bill_data.get('utility_type', ''),
            meter_number,
            bill_date,
            billing_start,
            str(round(float(total_cost) * 100))  # Convert to cents
        ]
        
        fingerprint_string = '|'.join(filter(None, fingerprint_parts))
        return hashlib.sha256(fingerprint_string.encode()).hexdigest()
        
    except Exception as e:
        print(f"Error creating fingerprint: {e}")
        return ""

def check_for_duplicates(hotel_id: str, new_bill_data: dict) -> Tuple[bool, List[Dict], str]:
    """Check if bill is duplicate by comparing with existing bills"""
    try:
        new_fingerprint = create_bill_fingerprint(new_bill_data)
        if not new_fingerprint:
            return False, [], "low"
        
        # Get existing bills for comparison
        bill_date = (
            new_bill_data.get('summary', {}).get('bill_date', '') or
            new_bill_data.get('raw_data', {}).get('billingPeriod', {}).get('endDate', '') or
            new_bill_data.get('raw_data', {}).get('billSummary', {}).get('billingPeriodEndDate', '')
        )
        
        year = bill_date[:4] if bill_date else str(datetime.now().year)
        existing_bills = get_utility_data_for_hotel_year(hotel_id, year)
        
        # Also check previous year for cross-year bills
        prev_year = str(int(year) - 1)
        existing_bills.extend(get_utility_data_for_hotel_year(hotel_id, prev_year))
        
        duplicates = []
        confidence = "low"
        
        for existing_bill in existing_bills:
            existing_fingerprint = create_bill_fingerprint(existing_bill)
            
            # Exact match
            if existing_fingerprint == new_fingerprint and existing_fingerprint:
                duplicates.append({
                    'filename': existing_bill.get('filename', ''),
                    'bill_date': existing_bill.get('summary', {}).get('bill_date', ''),
                    'uploaded_at': existing_bill.get('uploaded_at', ''),
                    'confidence': 'exact',
                    's3_key': existing_bill.get('s3_key', '')
                })
                confidence = "exact"
                continue
            
            # High confidence match - same meter, similar period, similar cost
            if is_high_confidence_duplicate(new_bill_data, existing_bill):
                duplicates.append({
                    'filename': existing_bill.get('filename', ''),
                    'bill_date': existing_bill.get('summary', {}).get('bill_date', ''),
                    'uploaded_at': existing_bill.get('uploaded_at', ''),
                    'confidence': 'high',
                    's3_key': existing_bill.get('s3_key', '')
                })
                if confidence != "exact":
                    confidence = "high"
        
        is_duplicate = len(duplicates) > 0
        return is_duplicate, duplicates, confidence
        
    except Exception as e:
        print(f"Error checking for duplicates: {e}")
        return False, [], "low"

def is_high_confidence_duplicate(bill1: dict, bill2: dict) -> bool:
    """Check if two bills are likely duplicates with high confidence"""
    try:
        # Must be same utility type
        if bill1.get('utility_type') != bill2.get('utility_type'):
            return False
        
        # Extract comparison data for bill1
        summary1 = bill1.get('summary', {})
        raw1 = bill1.get('raw_data', {})
        
        meter1 = (summary1.get('meter_number', '') or 
                 raw1.get('meterDetails', {}).get('meterNumber', '') or
                 raw1.get('accountInfo', {}).get('meterNumber', '')).strip().lower()
        
        date1 = (summary1.get('bill_date', '') or
                raw1.get('billingPeriod', {}).get('endDate', '') or
                raw1.get('billSummary', {}).get('billingPeriodEndDate', ''))
        
        cost1 = (summary1.get('total_cost', 0) or
                raw1.get('totalAmount', {}).get('value', 0) or
                raw1.get('billSummary', {}).get('currentBillAmount', 0))
        
        # Extract comparison data for bill2
        summary2 = bill2.get('summary', {})
        raw2 = bill2.get('raw_data', {})
        
        meter2 = (summary2.get('meter_number', '') or 
                 raw2.get('meterDetails', {}).get('meterNumber', '') or
                 raw2.get('accountInfo', {}).get('meterNumber', '')).strip().lower()
        
        date2 = (summary2.get('bill_date', '') or
                raw2.get('billingPeriod', {}).get('endDate', '') or
                raw2.get('billSummary', {}).get('billingPeriodEndDate', ''))
        
        cost2 = (summary2.get('total_cost', 0) or
                raw2.get('totalAmount', {}).get('value', 0) or
                raw2.get('billSummary', {}).get('currentBillAmount', 0))
        
        # Check conditions for high confidence duplicate
        same_meter = meter1 and meter2 and meter1 == meter2
        same_date = date1 and date2 and date1 == date2
        similar_cost = abs(float(cost1) - float(cost2)) <= 5.0  # Within €5
        
        return same_meter and same_date and similar_cost
        
    except Exception as e:
        print(f"Error in high confidence check: {e}")
        return False

# ============= EXISTING FUNCTIONS WITH DUPLICATE DETECTION (UNCHANGED) =============

@router.post("/precheck")
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

@router.post("/parse-and-save")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    file: UploadFile = File(...),
    bill_date: str = Form(...),
    bill_type: str = Form(...),
    supplier: str = Form(default="anthropic"),  # CHANGE: default from docupanda to anthropic
    force_upload: bool = Form(default=False)
):
    # CHANGE: Check Anthropic API key instead of DocuPipe
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="Anthropic API key not configured")
    
    if bill_type not in ["electricity", "gas"]:
        raise HTTPException(status_code=400, detail="Invalid bill type. Must be 'electricity' or 'gas'")
    
    content = await file.read()
    filename = file.filename
    
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    
    if not filename or not filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    # Quick duplicate pre-check (optional)
    if not force_upload:
        try:
            temp_bill_data = {
                'hotel_id': hotel_id,
                'utility_type': bill_type,
                'filename': filename,
                'summary': {'bill_date': bill_date},
                'raw_data': {}
            }
            
            is_duplicate, duplicates, confidence = check_for_duplicates(hotel_id, temp_bill_data)
            
            if is_duplicate and confidence == 'exact':
                return {
                    "status": "duplicate_detected",
                    "message": f"Exact duplicate detected",
                    "duplicates": duplicates,
                    "allow_force": True,
                    "original_filename": filename
                }
        except Exception as e:
            print(f"Pre-check duplicate detection failed: {e}")
    
    # CHANGE: Call Anthropic function instead of DocuPipe
    background_tasks.add_task(
        process_and_store_anthropic, 
        content, hotel_id, filename, bill_date, bill_type, supplier, force_upload
    )
    
    return {"status": "processing", "message": "Upload received. Processing in background."}

# CHANGE: Replace entire DocuPipe processing function with Anthropic
def process_and_store_anthropic(content, hotel_id, filename, bill_date, bill_type, supplier="anthropic", force_upload=False):
    try:
        print(f"Processing {filename} - Type: {bill_type}, Hotel: {hotel_id}, Force: {force_upload}")
        
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
        
        # CHANGE: Use SmartBillExtractor instead of DocuPipe
        print("Processing with SmartBillExtractor...")
        try:
            claude_result = process_with_smart_bill_extractor(content, bill_type, filename, ANTHROPIC_API_KEY)
            
            if not claude_result or "raw_data" not in claude_result:
                error_msg = "SmartBillExtractor failed to process the bill"
                print(error_msg)
                send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                return
            
            parsed = claude_result["raw_data"]
            validation_info = claude_result.get("validation", {})
            
            print(f"SmartBillExtractor completed with {validation_info.get('overall_confidence', 0)}% confidence")
            print(f"Got parsed data with keys: {list(parsed.keys())}")
            
        except Exception as e:
            error_msg = f"SmartBillExtractor processing error: {e}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        billing_start = (
            parsed.get("billingPeriod", {}).get("startDate") or 
            parsed.get("billSummary", {}).get("billingPeriodStartDate") or 
            bill_date
        )
        
        # CREATE COMPLETE BILL DATA FOR DUPLICATE CHECK
        complete_bill_data = {
            "hotel_id": hotel_id,
            "utility_type": bill_type,
            "filename": filename,
            "uploaded_at": datetime.utcnow().isoformat(),
            "summary": extract_bill_summary_from_real_data(parsed, bill_type),
            "raw_data": parsed,
            "validation": validation_info  # CHANGE: Add validation info from SmartBillExtractor
        }
        
        # DUPLICATE CHECK WITH COMPLETE DATA
        if not force_upload:
            is_duplicate, duplicates, confidence = check_for_duplicates(hotel_id, complete_bill_data)
            
            if is_duplicate:
                print(f"DUPLICATE DETECTED: {confidence} confidence, {len(duplicates)} existing bills")
                for dup in duplicates:
                    print(f"  - {dup['filename']} ({dup['confidence']})")
                
                # Block exact duplicates
                if confidence == "exact":
                    error_msg = f"Exact duplicate blocked: {duplicates[0]['filename']}"
                    print(error_msg)
                    send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "duplicate_blocked", error_msg)
                    return
                
                # Add duplicate warning for high confidence
                complete_bill_data["duplicate_warning"] = {
                    "confidence": confidence,
                    "similar_bills": duplicates,
                    "detected_at": datetime.utcnow().isoformat(),
                    "saved_anyway": True
                }
        
        try:
            s3_json_path = save_parsed_data_to_s3(hotel_id, bill_type, parsed, "", filename, complete_bill_data)
            save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
            
            print(f"Successfully saved: {s3_json_path}")
            
            webhook_status = "success"
            if complete_bill_data.get("duplicate_warning"):
                webhook_status = "success_with_duplicate_warning"
            
            send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_json_path, detected_supplier, webhook_status)
            return
            
        except Exception as e:
            error_msg = f"Failed to save data: {e}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
    except Exception as e:
        error_msg = f"Processing error: {e}"
        print(error_msg)
        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", "Unknown", "error", error_msg)
        import traceback
        traceback.print_exc()

def save_parsed_data_to_s3(hotel_id: str, bill_type: str, parsed_data: dict, upload_date: str, filename: str, complete_bill_data: dict = None):
    """Save parsed utility bill data to S3 with duplicate detection info"""
    try:
        if complete_bill_data:
            bill_data = complete_bill_data
        else:
            summary = extract_bill_summary_from_real_data(parsed_data, bill_type)
            bill_data = {
                "hotel_id": hotel_id,
                "utility_type": bill_type,
                "filename": filename,
                "uploaded_at": upload_date or datetime.utcnow().isoformat(),
                "summary": summary,
                "raw_data": parsed_data
            }
        
        bill_date = bill_data.get('summary', {}).get('bill_date', '')
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
        
        # Add S3 key to bill data for future reference
        bill_data['s3_key'] = s3_key
        
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
            consumption = data.get('consumption', [])
            day_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Day'), 0)
            night_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Night'), 0)
            
            summary.update({
                'day_kwh': day_kwh,
                'night_kwh': night_kwh,
                'total_kwh': day_kwh + night_kwh,
                
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
                        
                        # Add S3 key for reference
                        bill_data['s3_key'] = key
                        
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

# ============= ALL OTHER ENDPOINTS REMAIN EXACTLY THE SAME =============

@router.get("/{hotel_id}/duplicates/stats")
async def get_duplicate_stats(hotel_id: str):
    """Get duplicate detection statistics"""
    try:
        current_year = datetime.now().year
        years = [str(current_year), str(current_year - 1)]
        
        stats = {"electricity": 0, "gas": 0, "total_duplicates": 0}
        
        for year in years:
            bills = get_utility_data_for_hotel_year(hotel_id, year)
            
            for bill in bills:
                if bill.get("duplicate_warning"):
                    utility_type = bill.get("utility_type", "unknown")
                    if utility_type in stats:
                        stats[utility_type] += 1
                    stats["total_duplicates"] += 1
        
        return {"stats": [
            {"utility_type": "electricity", "duplicates": stats["electricity"]},
            {"utility_type": "gas", "duplicates": stats["gas"]},
            {"utility_type": "total", "duplicates": stats["total_duplicates"]}
        ]}
        
    except Exception as e:
        print(f"Error getting duplicate stats: {e}")
        return {"stats": []}

@router.get("/{hotel_id}/duplicates/list")
async def list_duplicate_bills(hotel_id: str, year: str = None):
    """List all bills flagged as potential duplicates"""
    try:
        current_year = datetime.now().year
        years_to_check = [year] if year else [str(current_year), str(current_year - 1)]
        
        duplicate_bills = []
        
        for check_year in years_to_check:
            bills = get_utility_data_for_hotel_year(hotel_id, check_year)
            
            for bill in bills:
                if bill.get("duplicate_warning"):
                    duplicate_info = {
                        "filename": bill.get("filename", ""),
                        "utility_type": bill.get("utility_type", ""),
                        "bill_date": bill.get("summary", {}).get("bill_date", ""),
                        "uploaded_at": bill.get("uploaded_at", ""),
                        "s3_key": bill.get("s3_key", ""),
                        "duplicate_warning": bill.get("duplicate_warning", {}),
                        "total_cost": bill.get("summary", {}).get("total_cost", 0)
                    }
                    duplicate_bills.append(duplicate_info)
        
        # Sort by upload date, newest first
        duplicate_bills.sort(key=lambda x: x.get("uploaded_at", ""), reverse=True)
        
        return {
            "hotel_id": hotel_id,
            "duplicate_bills": duplicate_bills,
            "total_count": len(duplicate_bills)
        }
        
    except Exception as e:
        print(f"Error listing duplicate bills: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list duplicates: {str(e)}")

@router.delete("/{hotel_id}/duplicates/remove")
async def remove_duplicate_bill(hotel_id: str, s3_key: str):
    """Remove a specific duplicate bill"""
    try:
        # Verify the bill exists and is marked as duplicate
        try:
            file_response = s3_client.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key
            )
            
            bill_data = json.loads(file_response['Body'].read())
            
            if not bill_data.get("duplicate_warning"):
                raise HTTPException(status_code=400, detail="Bill is not marked as duplicate")
            
            if bill_data.get("hotel_id") != hotel_id:
                raise HTTPException(status_code=403, detail="Access denied")
                
        except s3_client.exceptions.NoSuchKey:
            raise HTTPException(status_code=404, detail="Bill not found")
        
        # Delete the JSON file
        s3_client.delete_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key
        )
        
        # Try to delete corresponding PDF (optional)
        try:
            pdf_filename = bill_data.get("filename", "")
            if pdf_filename:
                bill_date = bill_data.get("summary", {}).get("billing_period_start", "")
                year = bill_date[:4] if bill_date else str(datetime.now().year)
                utility_type = bill_data.get("utility_type", "")
                
                pdf_key = f"{hotel_id}/{utility_type}/{year}/{pdf_filename}"
                s3_client.delete_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=pdf_key
                )
                print(f"Also deleted PDF: {pdf_key}")
        except Exception as e:
            print(f"Could not delete PDF: {e}")
        
        return {
            "success": True,
            "message": f"Duplicate bill removed: {bill_data.get('filename', 'unknown')}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error removing duplicate bill: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to remove duplicate: {str(e)}")

@router.post("/{hotel_id}/duplicates/clean")
async def clean_all_duplicates(hotel_id: str, background_tasks: BackgroundTasks, keep_newest: bool = True):
    """Clean all duplicate bills for a hotel (keep newest or oldest)"""
    try:
        background_tasks.add_task(perform_duplicate_cleanup, hotel_id, keep_newest)
        
        return {
            "success": True,
            "message": f"Duplicate cleanup started for {hotel_id}. {'Keeping newest' if keep_newest else 'Keeping oldest'} bills.",
            "status": "processing"
        }
        
    except Exception as e:
        print(f"Error starting duplicate cleanup: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start cleanup: {str(e)}")

def perform_duplicate_cleanup(hotel_id: str, keep_newest: bool = True):
    """Background task to clean up duplicate bills"""
    try:
        print(f"Starting duplicate cleanup for {hotel_id}")
        
        current_year = datetime.now().year
        years = [str(current_year), str(current_year - 1), str(current_year - 2)]
        
        all_bills = []
        for year in years:
            bills = get_utility_data_for_hotel_year(hotel_id, year)
            all_bills.extend(bills)
        
        # Group bills by fingerprint
        fingerprint_groups = {}
        for bill in all_bills:
            fingerprint = create_bill_fingerprint(bill)
            if fingerprint:
                if fingerprint not in fingerprint_groups:
                    fingerprint_groups[fingerprint] = []
                fingerprint_groups[fingerprint].append(bill)
        
        deleted_count = 0
        
        # Process each group
        for fingerprint, bills in fingerprint_groups.items():
            if len(bills) > 1:
                print(f"Found {len(bills)} bills with same fingerprint")
                
                # Sort by upload date
                bills.sort(key=lambda x: x.get("uploaded_at", ""), reverse=keep_newest)
                
                # Keep the first one (newest or oldest based on sort), delete the rest
                bills_to_delete = bills[1:]
                
                for bill_to_delete in bills_to_delete:
                    try:
                        s3_key = bill_to_delete.get("s3_key")
                        if s3_key:
                            s3_client.delete_object(
                                Bucket=S3_BUCKET_NAME,
                                Key=s3_key
                            )
                            print(f"Deleted duplicate: {bill_to_delete.get('filename', 'unknown')}")
                            deleted_count += 1
                            
                            # Try to delete corresponding PDF
                            try:
                                pdf_filename = bill_to_delete.get("filename", "")
                                if pdf_filename:
                                    bill_date = bill_to_delete.get("summary", {}).get("billing_period_start", "")
                                    year = bill_date[:4] if bill_date else str(datetime.now().year)
                                    utility_type = bill_to_delete.get("utility_type", "")
                                    
                                    pdf_key = f"{hotel_id}/{utility_type}/{year}/{pdf_filename}"
                                    s3_client.delete_object(
                                        Bucket=S3_BUCKET_NAME,
                                        Key=pdf_key
                                    )
                                    print(f"Also deleted PDF: {pdf_key}")
                            except Exception as e:
                                print(f"Could not delete PDF for {pdf_filename}: {e}")
                                
                    except Exception as e:
                        print(f"Error deleting bill {bill_to_delete.get('filename', 'unknown')}: {e}")
        
        print(f"Duplicate cleanup completed for {hotel_id}. Deleted {deleted_count} duplicate bills.")
        
        # Send webhook notification
        send_upload_webhook(
            hotel_id, 
            "cleanup", 
            f"duplicate_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}", 
            "", 
            "", 
            "System", 
            "cleanup_completed", 
            f"Deleted {deleted_count} duplicate bills"
        )
        
    except Exception as e:
        print(f"Error in duplicate cleanup: {e}")
        send_upload_webhook(
            hotel_id, 
            "cleanup", 
            "duplicate_cleanup_failed", 
            "", 
            "", 
            "System", 
            "cleanup_error", 
            str(e)
        )

# ============= EXISTING ENDPOINTS (UNCHANGED) =============

@router.get("/bill-pdf/{hotel_id}/{utility_type}/{year}/{filename}")
async def get_bill_pdf_direct(hotel_id: str, utility_type: str, year: str, filename: str, request: Request):
    """Download PDF for a specific bill using direct S3 path"""
    try:
        s3_key = f"{hotel_id}/{utility_type}/{year}/{filename}"
        
        print(f"Looking for PDF at S3 key: {s3_key}")
        
        try:
            pdf_response = s3_client.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key
            )
            
            pdf_content = pdf_response['Body'].read()
            
            return Response(
                content=pdf_content,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f"{'inline' if request.query_params.get('disposition') == 'inline' else 'attachment'}; filename={filename}",
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

@router.get("/bill-pdf/{bill_id}")
async def get_bill_pdf(bill_id: str, request: Request):
    """Download PDF for a specific bill"""
    try:
        parts = bill_id.split("_")
        if len(parts) < 3:
            raise HTTPException(status_code=400, detail="Invalid bill ID format")
        
        hotel_id = parts[0]
        utility_type = parts[1]
        bill_date = "_".join(parts[2:])
        
        year = bill_date[:4] if len(bill_date) >= 4 else str(datetime.now().year)
        bills = get_utility_data_for_hotel_year(hotel_id, year)
        
        target_bill = None
        for bill in bills:
            if (bill.get("utility_type") == utility_type and 
                bill.get("summary", {}).get("bill_date", "").startswith(bill_date[:7])):
                target_bill = bill
                break
        
        if not target_bill:
            raise HTTPException(status_code=404, detail="Bill not found")
        
        original_filename = target_bill.get("filename", "")
        if not original_filename:
            raise HTTPException(status_code=404, detail="Original filename not found")
        
        billing_start = target_bill.get("summary", {}).get("billing_period_start", "")
        billing_year = billing_start[:4] if billing_start else year
        
        pdf_key = f"{hotel_id}/{utility_type}/{billing_year}/{original_filename}"
        
        print(f"Looking for PDF at S3 key: {pdf_key}")
        
        try:
            pdf_response = s3_client.get_object(
                Bucket=S3_BUCKET_NAME,
                Key=pdf_key
            )
            
            pdf_content = pdf_response['Body'].read()
            
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
                    "Content-Disposition": f"{'inline' if request.query_params.get('disposition') == 'inline' else 'attachment'}; filename={filename}",
                    "Content-Length": str(len(pdf_content))
                }
            )
            
        except s3_client.exceptions.NoSuchKey:
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
                            "Content-Disposition": f"{'inline' if request.query_params.get('disposition') == 'inline' else 'attachment'}; filename={filename}",
                            "Content-Length": str(len(pdf_content))
                        }
                    )
                    
                except s3_client.exceptions.NoSuchKey:
                    continue
            
            raise HTTPException(status_code=404, detail=f"PDF file not found for bill {bill_id}")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting bill PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get bill PDF: {str(e)}")

@router.get("/{hotel_id}/bills")
async def get_raw_bills_data(hotel_id: str, year: str = None):
    """Get raw bill data with duplicate detection info"""
    try:
        current_year = datetime.now().year
        years_to_check = [year] if year else [str(current_year), str(current_year - 1)]
        
        all_bills = []
        duplicate_count = 0
        
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
                    
                    # Check if this bill has duplicate warnings
                    is_flagged_duplicate = bool(bill.get("duplicate_warning"))
                    if is_flagged_duplicate:
                        duplicate_count += 1
                    
                    enhanced_bill = {
                        **bill,
                        'utility_type': utility_type,
                        'is_flagged_duplicate': is_flagged_duplicate,
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
            "duplicate_bills": duplicate_count,
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

@router.get("/{hotel_id}/{year}")
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
                
                # Skip duplicates at the monthly level (keep first one found)
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
                        "bill_id": bill.get('filename', f'electricity_{month_key}'),
                        "is_duplicate": bool(bill.get("duplicate_warning"))
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
                        "bill_id": bill.get('filename', f'gas_{month_key}'),
                        "is_duplicate": bool(bill.get("duplicate_warning"))
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

@router.get("/debug/actual-structure/{hotel_id}")
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

@router.get("/debug/test-single-bill/{hotel_id}")
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
                "has_duplicate_warning": 'duplicate_warning' in first_bill,
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
