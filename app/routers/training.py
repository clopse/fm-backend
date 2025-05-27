from fastapi import APIRouter, Request, HTTPException
import json
import boto3
from datetime import datetime

router = APIRouter()
s3 = boto3.client("s3")
BUCKET_NAME = "jmk-project-uploads"

def _get_training_result_key(hotel_id: str):
    return f"{hotel_id}/training/fire.json"

@router.post("/api/training/submit")
async def save_training_result(request: Request):
    try:
        data = await request.json()
        hotel_id = data.get("hotel_id")
        if not hotel_id:
            raise HTTPException(status_code=400, detail="Missing hotel_id")

        key = _get_training_result_key(hotel_id)
        try:
            existing = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            history = json.loads(existing["Body"].read().decode("utf-8"))
        except s3.exceptions.NoSuchKey:
            history = []

        data["submitted_at"] = datetime.utcnow().isoformat()
        history.insert(0, data)
        history = history[:100]  # limit to latest 100 entries

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(history, indent=2),
            ContentType="application/json"
        )

        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save result: {e}")
