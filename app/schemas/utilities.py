from pydantic import BaseModel
from typing import Optional, Dict, Any

class UtilityUploadResponse(BaseModel):
    message: str
    file_path: str
    metadata_path: str
    parsed_data: Optional[Dict[str, Any]] = None
