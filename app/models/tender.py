import uuid
from sqlalchemy import Column, String, Date, Text
from sqlalchemy.dialects.postgresql import UUID
from app.database import Base

class Tender(Base):
    __tablename__ = "tenders"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    hotel_id = Column(String, nullable=False)
    category = Column(String, nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text)
    location = Column(String)
    due_date = Column(Date)
    file_path = Column(String)
