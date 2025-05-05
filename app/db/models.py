# app/db/models.py
from datetime import datetime

db = []  # simulate DB for now with a list, replace with real DB logic later

def save_parsed_data_to_db(hotel_id: str, utility_type: str, parsed_data: dict, s3_path: str):
    record = {
        "hotel_id": hotel_id,
        "utility_type": utility_type,
        "s3_path": s3_path,
        "parsed_at": datetime.utcnow().isoformat(),
        "fields": parsed_data  # store entire JSON for now
    }
    db.append(record)  # log to memory for now
    # Later: write to PostgreSQL / Supabase / DynamoDB
    return record
