# routers/water.py

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
import os
import requests
import boto3
from datetime import date, timedelta, datetime
import json

router = APIRouter()
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "jmk-project-uploads")
s3_client = boto3.client('s3')

# Map your hotel_id to Smartflow device IDs here:
WATER_DEVICES = {
    "hiex-dublincc": [222710131040, 222710251186],
    # Add more hotel_ids and device IDs as needed.
}

def get_smartflow_token() -> str:
    """Authenticate to Smartflow and return bearer token."""
    SMARTFLOW_USER = os.getenv("SMARTFLOW_USER")
    SMARTFLOW_PASS = os.getenv("SMARTFLOW_PASS")
    if not SMARTFLOW_USER or not SMARTFLOW_PASS:
        raise RuntimeError("SMARTFLOW_USER and SMARTFLOW_PASS env vars required.")
    resp = requests.post(
        "https://api.smartflowmonitoring.com/latest/users/login/token/",
        data={"username": SMARTFLOW_USER, "password": SMARTFLOW_PASS},
        timeout=15,
    )
    if not resp.ok:
        raise HTTPException(status_code=401, detail="Smartflow login failed.")
    return resp.json().get("access_token")

def fetch_smartflow_daily(device_id: int, date_str: str, token: str):
    """Get daily usage for one device."""
    url = (
        f"https://api.smartflowmonitoring.com/latest/device_data/{device_id}"
        f"?req_date={date_str}&date_type=timezone"
    )
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=15)
    if not r.ok:
        raise HTTPException(status_code=502, detail=f"Smartflow device error {device_id}: {r.text}")
    return r.json()

def s3_water_key(hotel_id: str):
    return f"utilities/{hotel_id}/smartflow_history.json"

@router.post("/sync/{hotel_id}", tags=["water"])
async def sync_water_usage(
    hotel_id: str,
    backfill_days: int = Query(0, description="Days of history to sync (default today only)"),
    background_tasks: BackgroundTasks = None
):
    """
    Sync SmartFlow water usage for hotel and store daily data to S3.
    Call with a POST to update, or set as a daily CRON/background job.
    """
    if hotel_id not in WATER_DEVICES:
        raise HTTPException(400, f"Unknown hotel_id {hotel_id}")

    def sync_job():
        try:
            token = get_smartflow_token()
        except Exception as e:
            raise HTTPException(500, f"Smartflow token error: {e}")
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
                    data = fetch_smartflow_daily(device_id, date_str, token)
                    usage = (
                        data.get("usage_data", {}).get("usage", 0)
                        or data.get("usage", 0)
                    )
                    breakdown[str(device_id)] = usage
                    day_sum += usage
                except Exception as e:
                    breakdown[str(device_id)] = None
            records.append({
                "date": date_str,
                "total_usage_liters": day_sum,
                "device_breakdown": breakdown,
            })
        # Load existing from S3 (avoid duplicates)
        s3_key = s3_water_key(hotel_id)
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            history = json.loads(obj["Body"].read())
        except s3_client.exceptions.NoSuchKey:
            history = []
        except Exception as e:
            raise HTTPException(500, f"S3 error: {e}")
        # Merge: overwrite by date
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

@router.get("/{hotel_id}/history", tags=["water"])
async def get_water_history(hotel_id: str):
    """Return all daily water usage from S3 for this hotel."""
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = obj["Body"].read().decode()
        return json.loads(history)
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

@router.get("/{hotel_id}/latest", tags=["water"])
async def get_water_latest(hotel_id: str):
    """Return latest day's usage for hotel."""
    s3_key = s3_water_key(hotel_id)
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        history = json.loads(obj["Body"].read())
        return history[-1] if history else {}
    except Exception as e:
        raise HTTPException(404, f"Could not fetch water summary: {e}")

@router.get("/{hotel_id}/monthly", tags=["water"])
async def get_monthly_water(
    hotel_id: str,
    rooms: int = Query(100, description="Number of rooms for per-room stats (default 100)")
):
    """
    Return monthly water usage for charting.
    Each object: {month, cubic_meters, per_room_m3, days}
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
