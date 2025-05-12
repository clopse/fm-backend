from fastapi import APIRouter, HTTPException
import boto3
import os
import json

router = APIRouter()
BUCKET = os.getenv("AWS_BUCKET_NAME")
s3 = boto3.client("s3")

@router.get("/history/all")
def get_all_compliance_history():
    prefix = "logs/compliance-history/"
    all_entries = []
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        
        # Check if there are any files
        contents = response.get("Contents", [])
        if not contents:
            # If no files, return empty entries array
            return {"entries": []}
            
        for obj in contents:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
                
            hotel_id = key.split("/")[-1].replace(".json", "")
            
            try:
                history_obj = s3.get_object(Bucket=BUCKET, Key=key)
                history_data = json.loads(history_obj["Body"].read())
                
                for task_id, records in history_data.items():
                    for entry in records:
                        all_entries.append({
                            "hotel_id": hotel_id,
                            "task_id": task_id,
                            **entry  # includes all the original fields
                        })
            except Exception as e:
                print(f"Error processing file {key}: {str(e)}")
                continue
        
        # Safe sorting
        def safe_sort_key(entry):
            # Try multiple date fields that might be in your data
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
