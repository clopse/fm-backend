from pydantic import BaseModel
from typing import List, Dict

class FileInfo(BaseModel):
    filename: str
    uploaded_at: str
    url: str

class ReportCategory(BaseModel):
    category: str
    years: Dict[str, List[FileInfo]]