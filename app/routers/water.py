# routers/water.py
from fastapi import APIRouter, HTTPException, BackgroundTasks
import os
import requests
import boto3
from datetime import date, timedelta
import json

router = APIRouter()
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "jmk-project-uploads")
s3_client = boto3.client('s3')

# Device IDs per hotel (expand this as needed)
WATER_DEVICES = {
    "hiex-dublincc": [222710131040, 222710251186],
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

@router.post("/water/sync/{hotel_id}")
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

@router.get("/water/{hotel_id}/history")
async def get_water_history(hotel_id: str):
    """Return all water usage for hotel (from S3)."""
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = obj["Body"].read().decode()
        return json.loads(history)
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

@router.get("/water/{hotel_id}/latest")
async def get_water_latest(hotel_id: str):
    """Return latest day's usage for hotel."""
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = json.loads(obj["Body"].read())
        return history[-1] if history else {}
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

@router.get("/water/{hotel_id}/monthly")
async def get_monthly_water(hotel_id: str, rooms: int = 100):
    """
    Returns monthly water usage, aggregated from daily SmartFlow S3 history.
    - Each object: {month, cubic_meters, per_room_m3, days}
    - Used for the main WaterChart component.
    """
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = json.loads(obj["Body"].read())
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

    from collections import defaultdict

    months = defaultdict(lambda: {"cubic_meters": 0, "per_room_m3": 0, "days": 0, "month": ""})
    for entry in history:
        month = entry["date"][:7]
        liters = entry.get("total_usage_liters") or 0
        months[month]["cubic_meters"] += liters / 1000
        months[month]["days"] += 1
        months[month]["month"] = month

    for m in months:
        months[m]["per_room_m3"] = round(months[m]["cubic_meters"] / rooms, 2)
        months[m]["cubic_meters"] = round(months[m]["cubic_meters"], 2)

    out = sorted(months.values(), key=lambda x: x["month"])
    return out
