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
    debug_info = {
        "bucket": BUCKET,
        "prefix": prefix,
        "s3_files_found": [],
        "errors": []
    }
    
    try:
        # List objects in the bucket with the given prefix
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        
        # Check if there are any files
        contents = response.get("Contents", [])
        if not contents:
            return {
                "entries": [],
                "debug": {
                    **debug_info,
                    "message": "No files found in S3 bucket with the given prefix"
                }
            }
        
        # Log all files found
        for obj in contents:
            key = obj["Key"]
            debug_info["s3_files_found"].append({
                "key": key,
                "size": obj.get("Size", 0),
                "last_modified": str(obj.get("LastModified", ""))
            })
            
            if not key.endswith(".json"):
                continue
                
            hotel_id = key.split("/")[-1].replace(".json", "")
            
            try:
                # Get the object content
                history_obj = s3.get_object(Bucket=BUCKET, Key=key)
                history_data = json.loads(history_obj["Body"].read())
                
                # Log the content structure
                debug_info[f"file_content_{key}"] = {
                    "keys": list(history_data.keys()),
                    "entry_count": sum(len(records) for records in history_data.values() if isinstance(records, list))
                }
                
                # Extract entries
                for task_id, records in history_data.items():
                    if not isinstance(records, list):
                        debug_info["errors"].append(f"Records for task {task_id} in {key} is not a list")
                        continue
                        
                    for entry in records:
                        all_entries.append({
                            "hotel_id": hotel_id,
                            "task_id": task_id,
                            **entry
                        })
            except Exception as e:
                error_msg = f"Error processing file {key}: {str(e)}"
                debug_info["errors"].append(error_msg)
                continue
        
        # Safe sorting
        def safe_sort_key(entry):
            for field in ["loggedAt", "uploaded_at", "confirmedAt"]:
                if entry.get(field):
                    return entry.get(field)
            return ""
            
        sorted_entries = sorted(all_entries, key=safe_sort_key, reverse=True)
        
        # Return the entries along with debug info
        return {
            "entries": sorted_entries,
            "debug": debug_info
        }
    except Exception as e:
        import traceback
        error_detail = str(e)
        tb = traceback.format_exc()
        debug_info["errors"].append(error_detail)
        debug_info["traceback"] = tb
        
        print(f"Error in compliance history: {error_detail}\n{tb}")
        return {
            "entries": [],
            "debug": debug_info,
            "error": f"Failed to load audit history: {str(e)}"
        }
