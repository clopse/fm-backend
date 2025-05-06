from sqlalchemy import Column, Integer, String, Float, Text, Date, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ParsedUtilityBill(Base):
    __tablename__ = "parsed_utility_bills"

    id = Column(Integer, primary_key=True, index=True)
    hotel_id = Column(String, index=True)
    utility_type = Column(String)  # electricity or gas
    billing_start = Column(String, nullable=True)  # string format like '2025-03-25'
    customer_ref = Column(String, nullable=True)
    billing_ref = Column(String, nullable=True)
    meter_number = Column(String, nullable=True)
    total_amount = Column(Float, nullable=True)
    s3_path = Column(String, nullable=False)
    raw_json = Column(JSON)  # stores the full DocuPanda result
