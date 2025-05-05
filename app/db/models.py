from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ParsedUtilityBill(Base):
    __tablename__ = "parsed_utility_bills"

    id = Column(Integer, primary_key=True, index=True)
    hotel_id = Column(String, index=True)
    utility_type = Column(String, index=True)
    s3_path = Column(String)
    parsed_at = Column(DateTime, default=datetime.utcnow)
    raw_json = Column(Text)
