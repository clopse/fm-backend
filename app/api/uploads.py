from fastapi import APIRouter, UploadFile, File
from app.email.reader import parse_pdf

router = APIRouter()

@router.post("/uploads/utilities")
async def upload_utility_file(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        parsed_data = parse_pdf(contents)
        return {"status": "success", "data": parsed_data}
    except Exception as e:
        return {"status": "error", "message": str(e)}
