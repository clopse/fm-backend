from app.db.models import ParsedUtilityBill
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import json

def save_parsed_data_to_db(
    db: Session,
    hotel_id: str,
    utility_type: str,
    parsed_data: dict,
    s3_path: str
):
    try:
        # Try extracting billing start date from multiple formats
        billing_start = (
            parsed_data.get("billingPeriod", {}).get("startDate") or
            parsed_data.get("billingPeriodStartDate") or
            parsed_data.get("billSummary", {}).get("billingPeriodStartDate")
        )

        customer_ref = parsed_data.get("customerRef") or parsed_data.get("accountInfo", {}).get("accountNumber")
        billing_ref = parsed_data.get("billingRef") or parsed_data.get("billSummary", {}).get("invoiceNumber")
        meter_number = (
            parsed_data.get("meterDetails", {}).get("meterNumber") or
            parsed_data.get("accountInfo", {}).get("meterNumber")
        )
        total_amount = (
            parsed_data.get("totalAmount", {}).get("value") or
            parsed_data.get("billSummary", {}).get("totalDueAmount")
        )

        record = ParsedUtilityBill(
            hotel_id=hotel_id,
            utility_type=utility_type,
            billing_start=billing_start,
            customer_ref=customer_ref,
            billing_ref=billing_ref,
            meter_number=meter_number,
            total_amount=total_amount,
            s3_path=s3_path,
            raw_json=json.dumps(parsed_data, ensure_ascii=False)
        )

        db.add(record)
        db.commit()
        db.refresh(record)
        return record

    except SQLAlchemyError as e:
        db.rollback()
        raise RuntimeError(f"Database error while saving utility bill: {str(e)}")
