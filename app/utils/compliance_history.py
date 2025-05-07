import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = 'jmk-project-uploads'

def _get_history_key(hotel_id: str) -> str:
    return f'logs/compliance-history/{hotel_id}.json'

def load_compliance_history(hotel_id: str) -> dict:
    key = _get_history_key(hotel_id)
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        print(f"[ERROR] Failed to load compliance history for {hotel_id}: {e}")
        return {}

def save_compliance_history(hotel_id: str, history: dict):
    key = _get_history_key(hotel_id)
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(history, indent=2),
            ContentType='application/json'
        )
    except Exception as e:
        print(f"[ERROR] Failed to save compliance history for {hotel_id}: {e}")

def add_history_entry(hotel_id: str, task_id: str, entry: dict):
    history = load_compliance_history(hotel_id)
    if task_id not in history:
        history[task_id] = []
    history[task_id].insert(0, entry)
    history[task_id] = history[task_id][:4]  # Keep max 4 entries
    save_compliance_history(hotel_id, history)

def delete_history_entry(hotel_id: str, task_id: str, timestamp: str):
    history = load_compliance_history(hotel_id)
    if task_id in history:
        history[task_id] = [
            e for e in history[task_id]
            if e.get('uploadedAt') != timestamp and e.get('confirmedAt') != timestamp
        ]
        save_compliance_history(hotel_id, history)
