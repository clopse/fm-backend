from pydantic import BaseModel
from datetime import date
from typing import Optional
from uuid import UUID

class TenderCreate(BaseModel):
    hotel_id: str
    category: str
    title: str
    description: Optional[str] = None
    location: Optional[str] = None
    due_date: date

class TenderOut(TenderCreate):
    id: UUID
    file_path: Optional[str] = None
