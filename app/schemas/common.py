from pydantic import BaseModel

class SuccessResponse(BaseModel):
    id: str
    message: str
