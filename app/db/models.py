from sqlalchemy import Column, Integer, String, Float, Date, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ParsedUtilityBill(Base):
    __tablename__ = "parsed_utility_bills"

    id = Column(Integer, primary_key=True, index=True)
    hotel_id = Column(String, index=True, nullable=False)
    utility_type = Column(String, index=True, nullable=False)

    billing_start = Column(String, nullable=True)  # You could use Date if format is guaranteed
    customer_ref = Column(String, nullable=True)
    billing_ref = Column(String, nullable=True)
    meter_number = Column(String, nullable=True)
    total_amount = Column(Float, nullable=True)

    s3_path = Column(String, nullable=False)
    raw_json = Column(Text, nullable=False)  # Stored as raw JSON string

    # Optional: Add created_at / updated_at columns
    # created_at = Column(DateTime, default=datetime.utcnow)
    # updated_at = Column(DateTime, onupdate=datetime.utcnow)
