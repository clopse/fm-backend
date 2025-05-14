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
        # Step 1: List all JSON history files in the compliance-history folder
        result = s3.list_objects_v2(Bucket=BUCKET, Prefix="logs/compliance-history/")
        files = result.get("Contents", [])

        print("✅ Found files in compliance-history folder:", [f["Key"] for f in files])

        for obj in files:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            try:
                hotel_id = key.split("/")[-1].replace(".json", "")
                print(f"🔍 Reading file: {key} for hotel: {hotel_id}")

                response = s3.get_object(Bucket=BUCKET, Key=key)
                history_data = json.loads(response["Body"].read())

                for task_id, records in history_data.items():
                    if not isinstance(records, list):
                        print(f"⚠️ Skipping {task_id} in {hotel_id}, not a list.")
                        continue

                    for entry in records:
                        full_entry = {
                            "hotel_id": hotel_id,
                            "task_id": task_id,
                            **entry
                        }
                        all_entries.append(full_entry)

            except Exception as e:
                print(f"❌ Error reading file {key}: {str(e)}")
                continue

        print(f"✅ Total entries loaded: {len(all_entries)}")

        # Safe sorting (fallback to uploaded_at/loggedAt)
        def sort_key(entry):
            return (
                entry.get("loggedAt") or
                entry.get("uploaded_at") or
                entry.get("confirmedAt") or
                ""
            )

        return {"entries": sorted(all_entries, key=sort_key, reverse=True)}

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"❌ Fatal error in /history/all: {str(e)}\n{tb}")
        raise HTTPException(status_code=500, detail="Failed to load compliance history")
