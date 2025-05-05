from app.db.models import ParsedUtilityBill
from sqlalchemy.orm import Session
import json

def save_parsed_data_to_db(db: Session, hotel_id: str, utility_type: str, parsed_data: dict, s3_path: str):
    record = ParsedUtilityBill(
        hotel_id=hotel_id,
        utility_type=utility_type,
        s3_path=s3_path,
        raw_json=json.dumps(parsed_data)
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record
