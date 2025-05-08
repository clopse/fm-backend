from fastapi import APIRouter, HTTPException
import boto3
import os
import json

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
                        **entry  # includes fileUrl, uploadedAt, confirmedAt, type, etc.
                    })

        return {"entries": sorted(all_entries, key=lambda x: x.get("uploadedAt") or x.get("confirmedAt"), reverse=True)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load audit history: {str(e)}")
