import boto3
import json
import os
from datetime import datetime
from typing import List, Dict, Any

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

def upload_to_s3(file_bytes: bytes, key: str) -> str:
    s3_client.put_object(Bucket=AWS_BUCKET_NAME, Key=key, Body=file_bytes)
    return f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{key}"

def generate_filename_from_dates(utility_type: str, start: str, end: str) -> str:
    return f"{utility_type}_{start}_to_{end}".replace("/", "-")

# NEW UTILITY FUNCTIONS
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
        
        # Fix filename extension properly
        base_filename = filename
        if base_filename.lower().endswith('.pdf'):
            base_filename = base_filename[:-4]  # Remove .pdf
        
        s3_key = f"utilities/{hotel_id}/{year}/{month}/{utility_type}_{base_filename}.json"
        
        # CHECK FOR DUPLICATES - but handle more carefully
        try:
            existing_file = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
            existing_data = json.loads(existing_file['Body'].read())
            
            # Compare key fields to detect duplicates
            existing_summary = existing_data.get("summary", {})
            new_summary = bill_data["summary"]
            
            # Check if it's the same bill (same dates, amounts, meter readings)
            is_duplicate = (
                existing_summary.get("bill_date") == new_summary.get("bill_date") and
                existing_summary.get("total_cost") == new_summary.get("total_cost") and
                existing_summary.get("account_number") == new_summary.get("account_number")
            )
            
            if is_duplicate:
                print(f"⚠️ Duplicate bill detected: {s3_key}")
                print(f"Existing: {existing_summary.get('bill_date')} - €{existing_summary.get('total_cost')}")
                print(f"New: {new_summary.get('bill_date')} - €{new_summary.get('total_cost')}")
                # Don't save, but don't error either - just return existing key
                return s3_key
            else:
                # Same filename but different bill - add timestamp to make unique
                timestamp = datetime.utcnow().strftime("%H%M%S")
                s3_key = f"utilities/{hotel_id}/{year}/{month}/{utility_type}_{base_filename}_{timestamp}.json"
                print(f"📄 Same filename, different bill content - saving as: {s3_key}")
                
        except s3_client.exceptions.NoSuchKey:
            # File doesn't exist - proceed with upload
            pass
        except Exception as e:
            print(f"Warning: Could not check for duplicates: {e}")
            # Continue with upload anyway
        
        # Save to S3
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
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
    """Get all utility bills for a hotel and year from S3 with daily averaging"""
    try:
        prefix = f"utilities/{hotel_id}/{year}/"
        
        response = s3_client.list_objects_v2(
            Bucket=AWS_BUCKET_NAME,
            Prefix=prefix
        )
        
        bills = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                try:
                    # Get the JSON file
                    file_response = s3_client.get_object(
                        Bucket=AWS_BUCKET_NAME,
                        Key=obj['Key']
                    )
                    
                    bill_data = json.loads(file_response['Body'].read())
                    
                    # Apply daily averaging to split bills across months
                    averaged_bills = apply_daily_averaging(bill_data)
                    bills.extend(averaged_bills)
                    
                except Exception as e:
                    print(f"Error reading {obj['Key']}: {e}")
                    continue
        
        # Group by month and aggregate
        monthly_data = {}
        for bill in bills:
            month_key = bill.get("month_key")
            if month_key not in monthly_data:
                monthly_data[month_key] = {
                    "electricity": {"total_cost": 0, "day_kwh": 0, "night_kwh": 0, "total_kwh": 0},
                    "gas": {"total_cost": 0, "consumption_kwh": 0}
                }
            
            # Aggregate the averaged data
            if bill["utility_type"] == "electricity":
                monthly_data[month_key]["electricity"]["total_cost"] += bill.get("daily_cost", 0) * bill.get("days_in_month", 0)
                monthly_data[month_key]["electricity"]["day_kwh"] += bill.get("daily_day_kwh", 0) * bill.get("days_in_month", 0)
                monthly_data[month_key]["electricity"]["night_kwh"] += bill.get("daily_night_kwh", 0) * bill.get("days_in_month", 0)
                monthly_data[month_key]["electricity"]["total_kwh"] += bill.get("daily_total_kwh", 0) * bill.get("days_in_month", 0)
            elif bill["utility_type"] == "gas":
                monthly_data[month_key]["gas"]["total_cost"] += bill.get("daily_cost", 0) * bill.get("days_in_month", 0)
                monthly_data[month_key]["gas"]["consumption_kwh"] += bill.get("daily_consumption_kwh", 0) * bill.get("days_in_month", 0)
        
        # Convert back to bill format for compatibility
        result_bills = []
        for month_key, data in monthly_data.items():
            if data["electricity"]["total_cost"] > 0:
                result_bills.append({
                    "utility_type": "electricity",
                    "summary": {
                        "bill_date": f"{month_key}-01",
                        "total_cost": round(data["electricity"]["total_cost"], 2),
                        "day_kwh": round(data["electricity"]["day_kwh"], 0),
                        "night_kwh": round(data["electricity"]["night_kwh"], 0),
                        "total_kwh": round(data["electricity"]["total_kwh"], 0)
                    }
                })
            
            if data["gas"]["total_cost"] > 0:
                result_bills.append({
                    "utility_type": "gas", 
                    "summary": {
                        "bill_date": f"{month_key}-01",
                        "total_cost": round(data["gas"]["total_cost"], 2),
                        "consumption_kwh": round(data["gas"]["consumption_kwh"], 0)
                    }
                })
        
        # Sort by bill date
        result_bills.sort(key=lambda x: x.get("summary", {}).get("bill_date") or "")
        return result_bills
        
    except Exception as e:
        print(f"Error fetching utility data: {e}")
        return []


def apply_daily_averaging(bill_data: dict) -> List[Dict]:
    """Split a bill across multiple months based on daily usage"""
    from datetime import datetime, timedelta
    
    summary = bill_data.get("summary", {})
    start_date_str = summary.get("billing_period_start") or summary.get("bill_date")
    end_date_str = summary.get("billing_period_end")
    
    if not start_date_str or not end_date_str:
        # No period info - return as single month bill
        month_key = start_date_str[:7] if start_date_str else datetime.now().strftime("%Y-%m")
        bill_copy = bill_data.copy()
        bill_copy["month_key"] = month_key
        bill_copy["days_in_month"] = 30  # Default
        return [bill_copy]
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        total_days = (end_date - start_date).days + 1
        
        if total_days <= 0:
            total_days = 30  # Fallback
        
        # Calculate daily rates
        total_cost = summary.get("total_cost", 0) or 0
        day_kwh = summary.get("day_kwh", 0) or 0
        night_kwh = summary.get("night_kwh", 0) or 0
        total_kwh = summary.get("total_kwh", 0) or summary.get("consumption_kwh", 0) or 0
        
        daily_cost = total_cost / total_days
        daily_day_kwh = day_kwh / total_days
        daily_night_kwh = night_kwh / total_days
        daily_total_kwh = total_kwh / total_days
        daily_consumption_kwh = total_kwh / total_days  # For gas bills
        
        # Split across months
        monthly_bills = []
        current_date = start_date
        
        while current_date <= end_date:
            month_key = current_date.strftime("%Y-%m")
            
            # Calculate days in this month for this bill
            month_start = max(current_date, datetime(current_date.year, current_date.month, 1))
            
            # Last day of current month
            if current_date.month == 12:
                next_month = datetime(current_date.year + 1, 1, 1)
            else:
                next_month = datetime(current_date.year, current_date.month + 1, 1)
            month_end = next_month - timedelta(days=1)
            
            # Days in this month for this bill
            bill_end_in_month = min(end_date, month_end)
            days_in_this_month = (bill_end_in_month - month_start).days + 1
            
            # Create monthly bill entry
            monthly_bill = {
                "utility_type": bill_data["utility_type"],
                "month_key": month_key,
                "days_in_month": days_in_this_month,
                "daily_cost": daily_cost,
                "daily_day_kwh": daily_day_kwh,
                "daily_night_kwh": daily_night_kwh,
                "daily_total_kwh": daily_total_kwh,
                "daily_consumption_kwh": daily_consumption_kwh,
                "original_bill": bill_data
            }
            
            monthly_bills.append(monthly_bill)
            
            # Move to next month
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)
        
        return monthly_bills
        
    except Exception as e:
        print(f"Error in daily averaging: {e}")
        # Fallback - return as single month
        month_key = start_date_str[:7] if start_date_str else datetime.now().strftime("%Y-%m")
        bill_copy = bill_data.copy()
        bill_copy["month_key"] = month_key
        bill_copy["days_in_month"] = 30
        return [bill_copy]


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
