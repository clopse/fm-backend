from fastapi import APIRouter, HTTPException, Request
from datetime import datetime, timedelta
import os
import json
import boto3
from app.routers.compliance_history import add_history_entry

router = APIRouter()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")

@router.get("/monthly-checklist/{hotel_id}")
async def get_monthly_checklist(hotel_id: str):
    """Get tasks that need monthly confirmation"""
    try:
        from app.s3_config import get_hotel_compliance_tasks
        all_tasks = get_hotel_compliance_tasks(hotel_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load compliance rules: {e}")

    now = datetime.utcnow()
    current_month = now.strftime("%Y-%m")
    
    monthly_tasks = []
    
    for task in all_tasks:
        task_id = task["task_id"]
        task_type = task.get("type", "upload")
        
        # Only include confirmation tasks for monthly checklist
        if task_type != "confirmation":
            continue
            
        # Check if already confirmed this month
        is_confirmed_this_month = False
        last_confirmed_date = None
        
        try:
            prefix = f"{hotel_id}/compliance/confirmations/{task_id}/"
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            
            for obj in resp.get("Contents", []):
                if obj["Key"].endswith(".json"):
                    meta = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                    data = json.loads(meta["Body"].read().decode("utf-8"))
                    
                    confirmed_at = data.get("confirmed_at") or data.get("report_date")
                    if confirmed_at:
                        confirmed_date = datetime.strptime(confirmed_at[:10], "%Y-%m-%d")
                        confirmed_month = confirmed_date.strftime("%Y-%m")
                        
                        # Update last confirmed date
                        if not last_confirmed_date or confirmed_date > last_confirmed_date:
                            last_confirmed_date = confirmed_date
                        
                        # Check if confirmed this month
                        if confirmed_month == current_month:
                            is_confirmed_this_month = True
                            
        except Exception:
            pass
        
        # Add task info for frontend
        task_info = {
            **task,  # Include all original task data
            "last_confirmed_date": last_confirmed_date.isoformat() if last_confirmed_date else None,
            "is_confirmed_this_month": is_confirmed_this_month
        }
        
        monthly_tasks.append(task_info)
    
    return monthly_tasks

@router.post("/confirm-task")
async def confirm_task(request: Request):
    """Confirm a task completion"""
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    user_email = data.get("user_email", "system@jmkfacilities.ie")
    
    if not hotel_id or not task_id:
        raise HTTPException(status_code=400, detail="Missing hotel_id or task_id")
    
    now = datetime.utcnow()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    
    # Create confirmation record
    confirmation_data = {
        "task_id": task_id,
        "hotel_id": hotel_id,
        "confirmed_at": now.isoformat(),
        "confirmed_by": user_email,
        "report_date": now.strftime("%Y-%m-%d"),  # For compatibility
        "type": "confirmation"
    }
    
    try:
        # Save confirmation to S3
        s3_key = f"{hotel_id}/compliance/confirmations/{task_id}/{timestamp}_confirmation.json"
        
        s3.put_object(
            Bucket=BUCKET,
            Key=s3_key,
            Body=json.dumps(confirmation_data, indent=2),
            ContentType="application/json"
        )
        
        # Add to compliance history
        history_entry = {
            **confirmation_data,
            "confirmedAt": now.isoformat(),  # Legacy format
            "confirmedBy": user_email        # Legacy format
        }
        
        add_history_entry(hotel_id, task_id, history_entry)
        
        return {"success": True, "message": "Task confirmed successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to confirm task: {str(e)}")
