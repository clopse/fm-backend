from fastapi import APIRouter, HTTPException
from typing import Dict, List
from app.services.storage_service import list_files, generate_signed_url  # <- Use signed URL function

router = APIRouter()

# List drawings for a hotel
@router.get("/drawings/{hotel_id}")
async def get_drawings(hotel_id: str) -> Dict[str, List[str]]:
    prefix = f"{hotel_id}/drawings/"

    try:
        objects = list_files(prefix)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list drawings: {str(e)}")

    if not objects:
        raise HTTPException(status_code=404, detail="No drawings found")

    categories = {}
    for obj in objects:
        key = obj["Key"]
        relative_key = key[len(prefix):]  # Remove 'hotel_id/drawings/' part
        parts = relative_key.split('/', 1)
        if len(parts) == 2:
            category, filename = parts
            if category not in categories:
                categories[category] = []
            categories[category].append(filename)

    return categories

# Get a specific drawing file (return signed URL)
@router.get("/drawings/{hotel_id}/{category}/{filename}")
async def get_drawing_file(hotel_id: str, category: str, filename: str):
    key = f"{hotel_id}/drawings/{category}/{filename}"

    try:
        file_url = generate_signed_url(key, expires_in=3600)  # Signed URL, valid for 1 hour
        return {"url": file_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate drawing link: {str(e)}")
