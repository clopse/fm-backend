from pydantic import BaseModel

class UtilityUploadResponse(BaseModel):
    message: str
    file_path: str
    metadata_path: str