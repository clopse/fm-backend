# backend/app/routers/hotel_facilities.py
from fastapi import APIRouter, HTTPException, Request
import json
import boto3
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

def _get_facilities_key(hotel_id: str) -> str:
    """Generate S3 key for hotel facilities data"""
    return f"hotels/facilities/{hotel_id}.json"

@router.get("/facilities/{hotel_id}")
async def get_hotel_facilities(hotel_id: str):
    """Get hotel facilities data"""
    key = _get_facilities_key(hotel_id)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return {"hotel_id": hotel_id, "facilities": data}
    except s3.exceptions.NoSuchKey:
        # Return minimal default structure - frontend will handle the rest
        return {
            "hotel_id": hotel_id,
            "facilities": {
                "hotelId": hotel_id,
                "hotelName": "",
                "address": "",
                "city": "",
                "county": "",
                "postCode": "",
                "phone": "",
                "managerName": "",
                "managerPhone": "",
                "managerEmail": "",
                "setupComplete": False,
                "lastUpdated": "",
                "updatedBy": ""
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load facilities data: {e}")

@router.post("/facilities/{hotel_id}")
async def save_hotel_facilities(hotel_id: str, request: Request):
    """Save hotel facilities data"""
    try:
        data = await request.json()
        
        # Add metadata
        data["hotelId"] = hotel_id
        data["lastUpdated"] = datetime.utcnow().isoformat()
        data["updatedBy"] = "Admin User"  # In real app, get from auth
        data["setupComplete"] = True
        
        # Save to S3
        key = _get_facilities_key(hotel_id)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        return {"success": True, "message": "Facilities data saved successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save facilities data: {e}")

@router.get("/facilities/all/summary")
async def get_all_facilities_summary():
    """Get summary of all hotel facilities"""
    hotels = ["hiex", "hida", "hbhdcc", "hbhe", "sera", "moxy"]
    
    summary = []
    for hotel_id in hotels:
        try:
            facilities_response = await get_hotel_facilities(hotel_id)
            facility_data = facilities_response.get("facilities", {})
            
            summary.append({
                "hotel_id": hotel_id,
                "hotel_name": facility_data.get("hotelName", hotel_id),
                "setup_complete": facility_data.get("setupComplete", False),
                "last_updated": facility_data.get("lastUpdated")
            })
        except Exception:
            summary.append({
                "hotel_id": hotel_id,
                "hotel_name": hotel_id,
                "setup_complete": False,
                "last_updated": None
            })
    
    return {"hotels": summary}
