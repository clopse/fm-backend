from sqlalchemy import Column, String, DateTime
from app.database import Base
import uuid
from datetime import datetime

class SafetyCheck(Base):
    __tablename__ = "safety_checks"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    hotel_id = Column(String, nullable=False)
    task_id = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    uploaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)
