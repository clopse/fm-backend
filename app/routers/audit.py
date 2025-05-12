from fastapi import APIRouter, HTTPException
import boto3
import os
import json

router = APIRouter()
BUCKET = os.getenv("AWS_BUCKET_NAME")
s3 = boto3.client("s3")

@router.get("/history/all")  # Changed from /audit/all to match the frontend URL
def get_all_compliance_history():
    prefix = "logs/compliance-history/"
    all_entries = []
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            hotel_id = key.split("/")[-1].replace(".json", "")
            history_obj = s3.get_object(Bucket=BUCKET, Key=key)
            history_data = json.loads(history_obj["Body"].read())
            for task_id, records in history_data.items():
                for entry in records:
                    # Convert field names to match the frontend
                    transformed_entry = {
                        "hotel_id": hotel_id,
                        "task_id": task_id,
                    }
                    
                    # Map the backend field names to the frontend field names
                    field_mapping = {
                        "uploaded_at": "uploadedAt",
                        "report_date": "reportDate",
                        # Keep other fields as is
                        "fileUrl": "fileUrl",
                        "filename": "filename", 
                        "confirmedAt": "confirmedAt",
                        "uploaded_by": "uploaded_by",
                        "type": "type",
                        "approved": "approved"
                    }
                    
                    for backend_field, frontend_field in field_mapping.items():
                        if backend_field in entry:
                            transformed_entry[frontend_field] = entry[backend_field]
                    
                    all_entries.append(transformed_entry)
        
        # Safe sorting function using frontend field names
        def safe_sort_key(entry):
            if entry.get("uploadedAt"):
                return entry.get("uploadedAt")
            if entry.get("confirmedAt"):
                return entry.get("confirmedAt")
            # Default fallback value for sorting
            return ""
            
        return {"entries": sorted(all_entries, key=safe_sort_key, reverse=True)}
    except Exception as e:
        import traceback
        error_detail = str(e)
        tb = traceback.format_exc()
        print(f"Error in compliance history: {error_detail}\n{tb}")
        raise HTTPException(status_code=500, detail=f"Failed to load audit history: {str(e)}")

@router.post("/history/approve")  # Endpoint for approving entries
def approve_compliance_entry(data: dict):
    try:
        hotel_id = data.get("hotel_id")
        task_id = data.get("task_id")
        timestamp = data.get("timestamp")
        
        if not all([hotel_id, task_id, timestamp]):
            raise HTTPException(status_code=400, detail="Missing required fields")
            
        # Load the existing JSON file
        key = f"logs/compliance-history/{hotel_id}.json"
        try:
            history_obj = s3.get_object(Bucket=BUCKET, Key=key)
            history_data = json.loads(history_obj["Body"].read())
        except Exception as e:
            raise HTTPException(status_code=404, detail=f"Could not find history for hotel: {hotel_id}")
            
        # Update the entry
        updated = False
        if task_id in history_data:
            for entry in history_data[task_id]:
                # Check if this is the entry to approve
                if entry.get("uploaded_at") == timestamp or entry.get("confirmedAt") == timestamp:
                    entry["approved"] = True
                    updated = True
                    
        if not updated:
            raise HTTPException(status_code=404, detail="Entry not found")
            
        # Save back to S3
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(history_data, indent=2),
            ContentType="application/json"
        )
        
        return {"success": True, "message": "Entry approved"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Approval failed: {str(e)}")
