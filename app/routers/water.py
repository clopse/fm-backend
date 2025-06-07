# routers/water.py
from fastapi import APIRouter, HTTPException, BackgroundTasks
import os
import requests
import boto3
from datetime import date, timedelta
import json
from collections import defaultdict

router = APIRouter()
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "jmk-project-uploads")
s3_client = boto3.client('s3')

# Device IDs per hotel (expand this as needed)
WATER_DEVICES = {
    "hiex": [222710131040, 222710251186],
    "hiex-dublincc": [222710131040, 222710251186],  # legacy support
    # add more hotel_ids and their device_ids here
}

SMARTFLOW_URL = os.getenv("SMARTFLOW_URL", "https://api.smartflowmonitoring.com")

def get_smartflow_token():
    """Login to SmartFlow API and return access token."""
    resp = requests.post(
        f"{SMARTFLOW_URL}/latest/users/login/token/",
        data={
            "username": os.getenv("SMARTFLOW_USER"),
            "password": os.getenv("SMARTFLOW_PASS")
        },
        timeout=15
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_smartflow_daily(device_id, date_str, token):
    """Fetch daily water usage for a device."""
    url = f"{SMARTFLOW_URL}/latest/devices/{device_id}/daily_flow?date={date_str}"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    # Usually the API returns {'date': ..., 'usage': ...} or similar
    usage = data.get("usage", 0) or data.get("total_usage_liters", 0) or 0
    return usage

def s3_water_key(hotel_id):
    return f"utilities/{hotel_id}/smartflow_history.json"

def s3_historical_water_key(hotel_id):
    """Key for the historical SmartFlow data"""
    return f"utilities/{hotel_id}/smartflow-usage.json"

@router.post("/sync/{hotel_id}")
async def sync_water_usage(
    hotel_id: str, 
    backfill_days: int = 0, 
    background_tasks: BackgroundTasks = None
):
    """Sync SmartFlow water usage for hotel, store JSON to S3 (run daily/cron)."""
    if hotel_id not in WATER_DEVICES:
        raise HTTPException(400, f"Unknown hotel_id {hotel_id}")

    def sync_job():
        try:
            token = get_smartflow_token()
        except Exception as e:
            raise HTTPException(502, f"SmartFlow login failed: {e}")
        today = date.today()
        days = backfill_days or 1
        records = []
        for delta in range(days):
            d = today - timedelta(days=delta)
            date_str = d.isoformat()
            day_sum = 0
            breakdown = {}
            for device_id in WATER_DEVICES[hotel_id]:
                try:
                    usage = fetch_smartflow_daily(device_id, date_str, token)
                    breakdown[str(device_id)] = usage
                    day_sum += usage
                except Exception as e:
                    breakdown[str(device_id)] = None
            records.append({
                "date": date_str,
                "total_usage_liters": day_sum,
                "device_breakdown": breakdown
            })
        # Merge with existing S3 data (avoid duplicates)
        s3_key = s3_water_key(hotel_id)
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            history = json.loads(obj["Body"].read())
        except s3_client.exceptions.NoSuchKey:
            history = []
        except Exception:
            history = []
        # merge: keep all unique dates
        all_dates = {r["date"]: r for r in history}
        for r in records:
            all_dates[r["date"]] = r
        new_history = [all_dates[d] for d in sorted(all_dates.keys())]
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(new_history, indent=2),
            ContentType="application/json"
        )

    if background_tasks:
        background_tasks.add_task(sync_job)
        return {"status": "started in background"}
    else:
        sync_job()
        return {"status": "synced"}

@router.get("/{hotel_id}/history")
async def get_water_history(hotel_id: str):
    """Return all water usage for hotel (from S3)."""
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = obj["Body"].read().decode()
        return json.loads(history)
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

@router.get("/{hotel_id}/latest")
async def get_water_latest(hotel_id: str):
    """Return latest day's usage for hotel."""
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = json.loads(obj["Body"].read())
        return history[-1] if history else {}
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

@router.get("/{hotel_id}/monthly")
async def get_monthly_water(hotel_id: str, rooms: int = 198):
    """
    Returns monthly water usage, aggregated from historical SmartFlow data.
    First tries to use smartflow-usage.json, falls back to daily data.
    - Each object: {month, cubic_meters, per_room_m3, days, device_breakdown}
    - Used for the main WaterChart component.
    """
    # First try to get historical data
    historical_key = s3_historical_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=historical_key)
        historical_data = json.loads(obj["Body"].read())
        
        # Process historical data
        months = defaultdict(lambda: {
            "cubic_meters": 0, 
            "per_room_m3": 0, 
            "days": 0, 
            "month": "",
            "device_breakdown": defaultdict(float)
        })
        
        for entry in historical_data:
            # Create month key (YYYY-MM format)
            month_key = f"{entry['Year']}-{entry['time_value']:02d}"
            device_id = str(entry['device_id'])
            usage_liters = entry.get('Usage', 0)
            
            months[month_key]["cubic_meters"] += usage_liters / 1000  # Convert to m³
            months[month_key]["month"] = month_key
            months[month_key]["days"] = 30  # Approximate for monthly data
            months[month_key]["device_breakdown"][device_id] += usage_liters / 1000
        
        # Calculate per room usage
        for month_data in months.values():
            month_data["per_room_m3"] = round(month_data["cubic_meters"] / rooms, 2)
            month_data["cubic_meters"] = round(month_data["cubic_meters"], 2)
            # Convert defaultdict to regular dict for JSON serialization
            month_data["device_breakdown"] = dict(month_data["device_breakdown"])
        
        result = sorted(months.values(), key=lambda x: x["month"])
        return result
        
    except s3_client.exceptions.NoSuchKey:
        # Fall back to daily data aggregation
        pass
    except Exception as e:
        print(f"Error reading historical data: {e}")
    
    # Fallback to original daily data approach
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = json.loads(obj["Body"].read())
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

    months = defaultdict(lambda: {
        "cubic_meters": 0, 
        "per_room_m3": 0, 
        "days": 0, 
        "month": "",
        "device_breakdown": defaultdict(float)
    })
    
    for entry in history:
        month = entry["date"][:7]  # YYYY-MM
        liters = entry.get("total_usage_liters") or 0
        device_breakdown = entry.get("device_breakdown", {})
        
        months[month]["cubic_meters"] += liters / 1000
        months[month]["days"] += 1
        months[month]["month"] = month
        
        # Aggregate device breakdown
        for device_id, device_usage in device_breakdown.items():
            if device_usage is not None:
                months[month]["device_breakdown"][device_id] += device_usage / 1000

    for month_data in months.values():
        month_data["per_room_m3"] = round(month_data["cubic_meters"] / rooms, 2)
        month_data["cubic_meters"] = round(month_data["cubic_meters"], 2)
        # Convert defaultdict to regular dict for JSON serialization
        month_data["device_breakdown"] = dict(month_data["device_breakdown"])

    result = sorted(months.values(), key=lambda x: x["month"])
    return result

@router.get("/{hotel_id}/device-breakdown/{month}")
async def get_device_breakdown(hotel_id: str, month: str):
    """
    Get device-level breakdown for a specific month.
    Month format: YYYY-MM
    """
    # First try historical data
    historical_key = s3_historical_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=historical_key)
        historical_data = json.loads(obj["Body"].read())
        
        year, month_num = month.split('-')
        year = int(year)
        month_num = int(month_num)
        
        devices = {}
        for entry in historical_data:
            if entry['Year'] == year and entry['time_value'] == month_num:
                device_id = str(entry['device_id'])
                devices[device_id] = {
                    "device_id": device_id,
                    "usage_liters": entry.get('Usage', 0),
                    "usage_m3": round(entry.get('Usage', 0) / 1000, 2),
                    "avg_usage_liters": entry.get('AvgUsage', 0),
                    "avg_usage_m3": round(entry.get('AvgUsage', 0) / 1000, 2)
                }
        
        return {
            "month": month,
            "devices": list(devices.values()),
            "total_m3": sum(d["usage_m3"] for d in devices.values())
        }
        
    except s3_client.exceptions.NoSuchKey:
        pass
    except Exception as e:
        print(f"Error reading historical data: {e}")
    
    # Fallback to daily data
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = json.loads(obj["Body"].read())
        
        devices = defaultdict(float)
        for entry in history:
            if entry["date"].startswith(month):
                device_breakdown = entry.get("device_breakdown", {})
                for device_id, usage in device_breakdown.items():
                    if usage is not None:
                        devices[device_id] += usage
        
        device_list = []
        for device_id, total_usage in devices.items():
            device_list.append({
                "device_id": device_id,
                "usage_liters": total_usage,
                "usage_m3": round(total_usage / 1000, 2)
            })
        
        return {
            "month": month,
            "devices": device_list,
            "total_m3": sum(d["usage_m3"] for d in device_list)
        }
        
    except Exception as e:
        raise HTTPException(404, f"Could not fetch device breakdown: {e}")

@router.get("/{hotel_id}/summary")
async def get_water_summary(hotel_id: str, rooms: int = 100):
    """
    Get a summary of water usage including totals, averages, and trends.
    """
    try:
        monthly_data = await get_monthly_water(hotel_id, rooms)
        
        if not monthly_data:
            return {"error": "No data available"}
        
        total_usage = sum(month["cubic_meters"] for month in monthly_data)
        avg_monthly = total_usage / len(monthly_data) if monthly_data else 0
        avg_per_room = sum(month["per_room_m3"] for month in monthly_data) / len(monthly_data) if monthly_data else 0
        
        # Calculate trend (last 3 months vs previous 3 months)
        trend = "stable"
        if len(monthly_data) >= 6:
            recent_avg = sum(month["cubic_meters"] for month in monthly_data[-3:]) / 3
            previous_avg = sum(month["cubic_meters"] for month in monthly_data[-6:-3]) / 3
            
            if recent_avg > previous_avg * 1.1:
                trend = "increasing"
            elif recent_avg < previous_avg * 0.9:
                trend = "decreasing"
        
        return {
            "total_usage_m3": round(total_usage, 2),
            "avg_monthly_m3": round(avg_monthly, 2),
            "avg_per_room_m3": round(avg_per_room, 2),
            "months_of_data": len(monthly_data),
            "trend": trend,
            "latest_month": monthly_data[-1] if monthly_data else None,
            "date_range": {
                "start": monthly_data[0]["month"] if monthly_data else None,
                "end": monthly_data[-1]["month"] if monthly_data else None
            }
        }
        
    except Exception as e:
        raise HTTPException(500, f"Could not generate summary: {e}")
