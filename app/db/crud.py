import boto3
import json
from datetime import datetime
from typing import List, Dict, Any
import os

# S3 client
s3_client = boto3.client('s3')
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")  # Your existing bucket

def save_parsed_data_to_s3(
    hotel_id: str,
    utility_type: str,
    parsed: dict,
    s3_json_path: str,
    filename: str
):
    """Save utility bill data as JSON file in S3"""
    try:
        # Extract key information for easy querying
        bill_data = {
            "hotel_id": hotel_id,
            "utility_type": utility_type,  # "gas" or "electricity"
            "filename": filename,
            "uploaded_at": datetime.utcnow().isoformat(),
            "s3_json_path": s3_json_path,
            
            # Standardized fields for easy comparison
            "summary": extract_summary_data(parsed, utility_type),
            
            # Full parsed data for detailed analysis
            "raw_data": parsed
        }
        
        # Create S3 key: utilities/HOTEL_ID/YEAR/MONTH/TYPE_FILENAME.json
        bill_date = bill_data["summary"].get("bill_date", datetime.utcnow().strftime("%Y-%m-%d"))
        year = bill_date[:4]
        month = bill_date[5:7]
        
        s3_key = f"utilities/{hotel_id}/{year}/{month}/{utility_type}_{filename.replace('.pdf', '.json')}"
        
        # Save to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(bill_data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        print(f"✅ Saved utility bill to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        print(f"❌ Failed to save to S3: {e}")
        raise RuntimeError(f"S3 save error: {str(e)}")


def extract_summary_data(parsed: dict, utility_type: str) -> dict:
    """Extract key fields for easy comparison across hotels"""
    
    if utility_type == "gas":
        bs = parsed.get("billSummary", {})
        cd = parsed.get("consumptionDetails", {})
        si = parsed.get("supplierInfo", {})
        
        return {
            "bill_date": bs.get("billingPeriodStartDate") or bs.get("issueDate"),
            "supplier": si.get("name"),
            "total_cost": bs.get("totalDueAmount") or bs.get("currentBillAmount"),
            "consumption_kwh": cd.get("consumptionValue"),
            "consumption_unit": cd.get("consumptionUnit"),
            "billing_period_start": bs.get("billingPeriodStartDate"),
            "billing_period_end": bs.get("billingPeriodEndDate"),
            "account_number": parsed.get("accountInfo", {}).get("accountNumber"),
            "meter_number": parsed.get("accountInfo", {}).get("meterNumber")
        }
        
    elif utility_type == "electricity":
        return {
            "bill_date": parsed.get("billingPeriod", {}).get("startDate") or parsed.get("issueDate"),
            "supplier": parsed.get("supplier"),
            "total_cost": parsed.get("totalAmount", {}).get("value"),
            "day_kwh": get_consumption_by_type(parsed, "day"),
            "night_kwh": get_consumption_by_type(parsed, "night"),
            "total_kwh": (get_consumption_by_type(parsed, "day") or 0) + (get_consumption_by_type(parsed, "night") or 0),
            "billing_period_start": parsed.get("billingPeriod", {}).get("startDate"),
            "billing_period_end": parsed.get("billingPeriod", {}).get("endDate"),
            "account_number": parsed.get("customerRef"),
            "meter_number": parsed.get("meterDetails", {}).get("meterNumber")
        }
    
    return {}


def get_consumption_by_type(parsed: dict, consumption_type: str) -> float:
    """Extract consumption value by type (day/night/wattless)"""
    consumption = parsed.get("consumption", [])
    for entry in consumption:
        if entry.get("type", "").lower() == consumption_type:
            return entry.get("units", {}).get("value")
    return None


def get_utility_data_for_hotel_year(hotel_id: str, year: str) -> List[Dict[Any, Any]]:
    """Get all utility bills for a hotel and year from S3"""
    try:
        prefix = f"utilities/{hotel_id}/{year}/"
        
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix
        )
        
        bills = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                try:
                    # Get the JSON file
                    file_response = s3_client.get_object(
                        Bucket=BUCKET_NAME,
                        Key=obj['Key']
                    )
                    
                    bill_data = json.loads(file_response['Body'].read())
                    bills.append(bill_data)
                    
                except Exception as e:
                    print(f"Error reading {obj['Key']}: {e}")
                    continue
        
        # Sort by bill date
        bills.sort(key=lambda x: x.get("summary", {}).get("bill_date") or "")
        return bills
        
    except Exception as e:
        print(f"Error fetching utility data: {e}")
        return []


def get_utility_data_for_multiple_hotels(hotel_ids: List[str], year: str) -> Dict[str, List[Dict]]:
    """Get utility data for multiple hotels for comparison"""
    result = {}
    
    for hotel_id in hotel_ids:
        result[hotel_id] = get_utility_data_for_hotel_year(hotel_id, year)
    
    return result


def get_utility_summary_for_comparison(hotel_ids: List[str], year: str) -> List[Dict]:
    """Get simplified data for hotel comparison charts"""
    comparison_data = []
    
    for hotel_id in hotel_ids:
        bills = get_utility_data_for_hotel_year(hotel_id, year)
        
        # Aggregate by hotel
        electricity_total = sum(
            bill["summary"].get("total_cost", 0) or 0 
            for bill in bills 
            if bill["utility_type"] == "electricity" and bill["summary"].get("total_cost")
        )
        
        gas_total = sum(
            bill["summary"].get("total_cost", 0) or 0 
            for bill in bills 
            if bill["utility_type"] == "gas" and bill["summary"].get("total_cost")
        )
        
        electricity_kwh = sum(
            bill["summary"].get("total_kwh", 0) or 0 
            for bill in bills 
            if bill["utility_type"] == "electricity" and bill["summary"].get("total_kwh")
        )
        
        gas_kwh = sum(
            bill["summary"].get("consumption_kwh", 0) or 0 
            for bill in bills 
            if bill["utility_type"] == "gas" and bill["summary"].get("consumption_kwh")
        )
        
        comparison_data.append({
            "hotel_id": hotel_id,
            "electricity_cost": round(electricity_total, 2),
            "gas_cost": round(gas_total, 2),
            "electricity_kwh": round(electricity_kwh, 0),
            "gas_kwh": round(gas_kwh, 0),
            "total_cost": round(electricity_total + gas_total, 2),
            "bill_count": len(bills)
        })
    
    return comparison_data
