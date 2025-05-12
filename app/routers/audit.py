from fastapi import APIRouter, HTTPException
import boto3
import os
import json

router = APIRouter()
BUCKET = os.getenv("AWS_BUCKET_NAME")
s3 = boto3.client("s3")

@router.get("/history/all")
def get_all_compliance_history():
    all_entries = []
    try:
        # Read the specific file we know exists
        key = "logs/compliance-history/hiex.json"
        try:
            history_obj = s3.get_object(Bucket=BUCKET, Key=key)
            history_data = json.loads(history_obj["Body"].read())
            
            hotel_id = "hiex"  # Hardcoded for now since we know the file name
            
            for task_id, records in history_data.items():
                if not isinstance(records, list):
                    continue
                    
                for entry in records:
                    all_entries.append({
                        "hotel_id": hotel_id,
                        "task_id": task_id,
                        **entry  # Include all fields from the entry
                    })
                    
        except s3.exceptions.NoSuchKey:
            print(f"File {key} not found in bucket {BUCKET}")
            pass  # Skip if file doesn't exist
        except Exception as e:
            print(f"Error processing file {key}: {str(e)}")
            pass  # Skip if there's an error
        
        # Safe sorting
        def safe_sort_key(entry):
            # Try multiple date fields that might be in the data
            for field in ["loggedAt", "uploaded_at", "confirmedAt"]:
                if entry.get(field):
                    return entry.get(field)
            return ""
            
        return {"entries": sorted(all_entries, key=safe_sort_key, reverse=True)}
    except Exception as e:
        import traceback
        error_detail = str(e)
        tb = traceback.format_exc()
        print(f"Error in compliance history: {error_detail}\n{tb}")
        raise HTTPException(status_code=500, detail=f"Failed to load audit history: {str(e)}")

@router.post("/history/approve")
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
        if task_id in history_data:
            updated = False
            for entry in history_data[task_id]:
                # Check if this is the entry to approve (match by uploaded_at timestamp)
                if entry.get("uploaded_at") == timestamp:
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
        else:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found for hotel {hotel_id}")
            
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = str(e)
        tb = traceback.format_exc()
        print(f"Error approving entry: {error_detail}\n{tb}")
        raise HTTPException(status_code=500, detail=f"Approval failed: {str(e)}")
