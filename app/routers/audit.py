from fastapi import APIRouter, HTTPException
import boto3
import os
import json
from datetime import datetime

router = APIRouter()
BUCKET = os.getenv("AWS_BUCKET_NAME")
s3 = boto3.client("s3")

@router.get("/audit/all")
def get_all_compliance_audit():
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
                    all_entries.append({
                        "hotel_id": hotel_id,
                        "task_id": task_id,
                        **entry  # includes fileUrl, uploaded_at, confirmedAt, type, etc.
                    })
        
        # Safe sorting that won't crash if date fields are missing
        def safe_sort_key(entry):
            # Try different date fields that might be in your data
            for field in ["loggedAt", "uploaded_at", "confirmedAt"]:
                if entry.get(field):
                    return entry.get(field)
            # Return epoch time if no date found, to put entries without dates at the end
            return ""
            
        return {"entries": sorted(all_entries, key=safe_sort_key, reverse=True)}
    except Exception as e:
        import traceback
        error_detail = str(e)
        tb = traceback.format_exc()
        print(f"Error in audit: {error_detail}\n{tb}")
        raise HTTPException(status_code=500, detail=f"Failed to load audit history: {str(e)}")
