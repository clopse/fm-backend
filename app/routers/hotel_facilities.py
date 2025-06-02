# FILE: backend/app/routers/hotel_facilities.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Dict, Any, Optional
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

# Debug: Print environment variables (remove in production)
print(f"AWS_BUCKET_NAME: {BUCKET_NAME}")
print(f"AWS_REGION: {os.getenv('AWS_REGION')}")
print(f"AWS_ACCESS_KEY_ID: {'SET' if os.getenv('AWS_ACCESS_KEY_ID') else 'NOT SET'}")
print(f"AWS_SECRET_ACCESS_KEY: {'SET' if os.getenv('AWS_SECRET_ACCESS_KEY') else 'NOT SET'}")

# Pydantic models for request bodies
class HotelFacilitiesData(BaseModel):
    hotelId: str
    hotelName: Optional[str] = ""
    address: Optional[str] = ""
    city: Optional[str] = ""
    county: Optional[str] = ""
    postCode: Optional[str] = ""
    phone: Optional[str] = ""
    managerName: Optional[str] = ""
    managerPhone: Optional[str] = ""
    managerEmail: Optional[str] = ""
    setupComplete: Optional[bool] = False
    lastUpdated: Optional[str] = ""
    updatedBy: Optional[str] = ""
    # Allow additional fields for structural, mechanical, etc.
    structural: Optional[Dict[str, Any]] = {}
    fireSafety: Optional[Dict[str, Any]] = {}
    mechanical: Optional[Dict[str, Any]] = {}
    utilities: Optional[Dict[str, Any]] = {}
    compliance: Optional[Dict[str, Any]] = {}

class HotelDetailsData(BaseModel):
    hotelId: str
    structural: Optional[Dict[str, Any]] = {}
    fireSafety: Optional[Dict[str, Any]] = {}
    mechanical: Optional[Dict[str, Any]] = {}
    utilities: Optional[Dict[str, Any]] = {}
    lastUpdated: Optional[str] = ""
    updatedBy: Optional[str] = ""

def get_facilities_key(hotel_id: str) -> str:
    """Generate S3 key for hotel facilities data"""
    return f"hotels/facilities/{hotel_id}.json"

def get_details_key(hotel_id: str) -> str:
    """Generate S3 key for hotel details data - now stored in compliance folder"""
    return f"{hotel_id}/compliance/details.json"

def get_compliance_key(hotel_id: str) -> str:
    """Generate S3 key for hotel compliance tasks"""
    return f"{hotel_id}/compliance/tasks.json"

@router.get("/facilities/{hotel_id}")
async def get_hotel_facilities(hotel_id: str):
    """Get hotel facilities data"""
    key = get_facilities_key(hotel_id)
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
async def save_hotel_facilities(hotel_id: str, facilities_data: HotelFacilitiesData):
    """Save hotel facilities data"""
    try:
        print(f"Saving facilities for hotel: {hotel_id}")
        print(f"Received facilities data: {facilities_data}")
        
        # Convert Pydantic model to dict
        data = facilities_data.dict()
        
        # Add metadata
        data["hotelId"] = hotel_id
        data["lastUpdated"] = datetime.utcnow().isoformat()
        data["updatedBy"] = "Admin User"  # In real app, get from auth
        data["setupComplete"] = True
        
        # Save to S3
        key = get_facilities_key(hotel_id)
        print(f"Saving facilities to S3 key: {key}")
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        print("Successfully saved facilities to S3")
        return {"success": True, "message": "Facilities data saved successfully"}
        
    except Exception as e:
        print(f"Error saving facilities: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to save facilities data: {str(e)}")

@router.get("/details/{hotel_id}")
async def get_hotel_details(hotel_id: str):
    """Get hotel details data (equipment, structure, etc.) from compliance folder"""
    key = get_details_key(hotel_id)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return {"hotel_id": hotel_id, "details": data}
    except s3.exceptions.NoSuchKey:
        # Return default structure if file doesn't exist
        return {
            "hotel_id": hotel_id, 
            "details": {
                "hotelId": hotel_id,
                "equipment": {},
                "structure": {},
                "lastUpdated": "",
                "updatedBy": ""
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load hotel details: {e}")

@router.post("/details/{hotel_id}")
async def save_hotel_details(hotel_id: str, details_data: HotelDetailsData):
    """Save hotel details data (equipment, structure, etc.) to compliance folder"""
    try:
        # Debug logging
        print(f"Saving details for hotel: {hotel_id}")
        print(f"Received details data: {details_data}")
        
        # Convert Pydantic model to dict
        data = details_data.dict()
        
        # Add metadata
        data["hotelId"] = hotel_id
        data["lastUpdated"] = datetime.utcnow().isoformat()
        data["updatedBy"] = "Admin User"  # In real app, get from auth
        
        # Save to S3 in the compliance folder
        key = get_details_key(hotel_id)
        print(f"Saving to S3 key: {key}")
        print(f"S3 Bucket: {BUCKET_NAME}")
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        print("Successfully saved to S3")
        return {"success": True, "message": "Hotel details saved successfully"}
        
    except Exception as e:
        print(f"Error saving hotel details: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to save hotel details: {str(e)}")

@router.get("/compliance/{hotel_id}")
async def get_hotel_compliance(hotel_id: str):
    """Get hotel compliance tasks"""
    key = get_compliance_key(hotel_id)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return {"hotel_id": hotel_id, "tasks": data}
    except s3.exceptions.NoSuchKey:
        return {"hotel_id": hotel_id, "tasks": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance tasks: {e}")

@router.post("/compliance/{hotel_id}")
async def save_hotel_compliance(hotel_id: str, request: Request):
    """Save hotel compliance tasks"""
    try:
        task_list = await request.json()
        
        # Add metadata
        compliance_data = {
            "hotelId": hotel_id,
            "lastUpdated": datetime.utcnow().isoformat(),
            "updatedBy": "Admin User",
            "tasks": task_list
        }
        
        # Save to S3 in the compliance folder
        key = get_compliance_key(hotel_id)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(compliance_data, indent=2),
            ContentType="application/json"
        )
        
        return {"success": True, "message": "Compliance tasks saved successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save compliance tasks: {e}")

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
