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
COMPLIANCE_RULES_PATH = "app/data/compliance.json"

def _get_facilities_key(hotel_id: str) -> str:
    """Generate S3 key for hotel facilities data"""
    return f"hotels/facilities/{hotel_id}.json"

def load_compliance_rules():
    """Load compliance rules from JSON file"""
    try:
        with open(COMPLIANCE_RULES_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load compliance rules: {e}")

def generate_applicable_tasks(facility_data: dict) -> list:
    """Generate list of applicable compliance tasks based on facility data"""
    rules = load_compliance_rules()
    applicable_tasks = []
    
    for section in rules:
        for task in section.get("tasks", []):
            task_id = task["task_id"]
            is_applicable = True
            
            # Determine if task applies based on facility data
            if task_id == "fire_risk_assessment":
                # Always applicable for hotels
                pass
            elif task_id == "fire_alarm_service_certificate":
                # Applicable if hotel has fire alarm system
                is_applicable = facility_data.get("fireSafety", {}).get("fireAlarmSystem", False)
            elif task_id == "fire_extinguisher_certificate":
                # Applicable if hotel has fire extinguishers
                is_applicable = facility_data.get("fireSafety", {}).get("fireExtinguishers", 0) > 0
            elif task_id == "emergency_light_cert":
                # Applicable if hotel has emergency lighting
                is_applicable = facility_data.get("fireSafety", {}).get("emergencyLighting", False)
            elif task_id == "sprinkler_service_certificate":
                # Applicable if hotel has sprinkler system
                is_applicable = facility_data.get("fireSafety", {}).get("sprinklerHeads", 0) > 0
            elif task_id == "dry_riser_test_certificate":
                # Applicable if hotel has dry risers (now emergency stairs)
                is_applicable = facility_data.get("fireSafety", {}).get("emergencyStairs", 0) > 0
            elif task_id == "ansul_system_check":
                # Applicable if hotel has commercial kitchens with ansul systems
                is_applicable = facility_data.get("mechanical", {}).get("ansulSystems", 0) > 0
            elif task_id == "passenger_lift_cert":
                # Applicable if hotel has elevators
                is_applicable = facility_data.get("mechanical", {}).get("elevators", 0) > 0
            elif task_id == "gas_safety_certificate":
                # Applicable if hotel has gas systems
                is_applicable = facility_data.get("utilities", {}).get("gasSupply", False)
            elif task_id == "boiler_service":
                # Applicable if hotel has boilers
                is_applicable = facility_data.get("mechanical", {}).get("boilers", 0) > 0
            elif task_id == "legionella_risk_assessment":
                # Always applicable for hotels with water systems
                pass
            elif task_id == "tank_inspection_annual":
                # Applicable if hotel has water storage tanks
                is_applicable = facility_data.get("utilities", {}).get("waterStorageTanks", 0) > 0
            elif task_id == "tmv_annual_service":
                # Applicable if hotel has TMVs
                is_applicable = facility_data.get("utilities", {}).get("thermostaticMixingValves", 0) > 0
            elif task_id == "food_handler_training_log":
                # Applicable if hotel has commercial kitchens
                is_applicable = facility_data.get("mechanical", {}).get("commercialKitchens", 0) > 0
            elif task_id == "pest_control_inspection":
                # Applicable if hotel has food service
                is_applicable = facility_data.get("mechanical", {}).get("commercialKitchens", 0) > 0
            elif task_id == "fridge_temp_log":
                # Applicable if hotel has commercial kitchens
                is_applicable = facility_data.get("mechanical", {}).get("commercialKitchens", 0) > 0
            elif task_id == "eicr_certificate":
                # Always applicable for hotels
                pass
            elif task_id == "pat_testing":
                # Always applicable for hotels
                pass
            # Add more conditions as needed
            
            if is_applicable:
                applicable_tasks.append({
                    **task,
                    "hotel_id": facility_data.get("hotelId"),
                    "applicable": True,
                    "reason": f"Based on facility configuration"
                })
    
    return applicable_tasks

@router.get("/facilities/{hotel_id}")
async def get_hotel_facilities(hotel_id: str):
    """Get hotel facilities data"""
    key = _get_facilities_key(hotel_id)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return {"hotel_id": hotel_id, "facilities": data}
    except s3.exceptions.NoSuchKey:
        # Return default structure if no data exists
        return {
            "hotel_id": hotel_id,
            "facilities": {
                "hotelId": hotel_id,
                "hotelName": "",
                "address": "",
                "city": "",
                "postCode": "",
                "phone": "",
                "managerName": "",
                "managerPhone": "",
                "managerEmail": "",
                "structural": {
                    "floors": 0,
                    "totalRooms": 0,
                    "buildingType": "",
                    "constructionYear": 0
                },
                "fireSafety": {
                    "fireAlarmSystem": False,
                    "fireExtinguishers": 0,
                    "emergencyLighting": False,
                    "sprinklerHeads": 0,
                    "emergencyStairs": 0,
                    "smokeDetectors": 0
                },
                "mechanical": {
                    "elevators": 0,
                    "boilers": 0,
                    "hvacUnits": 0,
                    "generators": 0,
                    "commercialKitchens": 0,
                    "ansulSystems": 0,
                    "poolPumps": 0
                },
                "utilities": {
                    "electricalSupply": "",
                    "gasSupply": False,
                    "waterSupply": "",
                    "sewerConnection": "",
                    "internetProvider": "",
                    "waterStorageTanks": 0,
                    "thermostaticMixingValves": 0
                },
                "compliance": {
                    "requiresAnsulService": False,
                    "requiresElevatorInspection": False,
                    "requiresBoilerInspection": False,
                    "requiresFireSystemInspection": False,
                    "requiresPoolInspection": False,
                    "requiresKitchenHoodCleaning": False,
                    "requiresGeneratorService": False,
                    "requiresHVACService": False
                },
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

@router.get("/facilities/{hotel_id}/applicable-tasks")
async def get_applicable_tasks(hotel_id: str):
    """Get compliance tasks applicable to this hotel based on facilities"""
    # First get the facilities data
    facilities_response = await get_hotel_facilities(hotel_id)
    facility_data = facilities_response.get("facilities", {})
    
    if not facility_data.get("setupComplete"):
        raise HTTPException(
            status_code=400, 
            detail="Hotel facilities setup not complete. Please complete the facilities questionnaire first."
        )
    
    # Generate applicable tasks
    applicable_tasks = generate_applicable_tasks(facility_data)
    
    return {
        "hotel_id": hotel_id,
        "total_tasks": len(applicable_tasks),
        "tasks": applicable_tasks
    }

@router.get("/facilities/all/summary")
async def get_all_facilities_summary():
    """Get summary of all hotel facilities"""
    # This would typically come from a database query
    # For now, we'll check S3 for existing facilities files
    hotels = ["hiex", "hida", "hbhdcc", "hbhe", "sera", "moxy"]  # Your hotel list
    
    summary = []
    for hotel_id in hotels:
        try:
            facilities_response = await get_hotel_facilities(hotel_id)
            facility_data = facilities_response.get("facilities", {})
            
            summary.append({
                "hotel_id": hotel_id,
                "hotel_name": facility_data.get("hotelName", hotel_id),
                "setup_complete": facility_data.get("setupComplete", False),
                "last_updated": facility_data.get("lastUpdated"),
                "total_rooms": facility_data.get("structural", {}).get("totalRooms", 0),
                "floors": facility_data.get("structural", {}).get("floors", 0)
            })
        except Exception:
            summary.append({
                "hotel_id": hotel_id,
                "hotel_name": hotel_id,
                "setup_complete": False,
                "last_updated": None,
                "total_rooms": 0,
                "floors": 0
            })
    
    return {"hotels": summary}
