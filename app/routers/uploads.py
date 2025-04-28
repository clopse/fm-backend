from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pathlib import Path
import os

router = APIRouter()

BASE_STORAGE_PATH = Path("storage")

@router.get("/drawings/{hotel_id}")
async def get_drawings(hotel_id: str):
    hotel_path = BASE_STORAGE_PATH / hotel_id / "drawings"
    if not hotel_path.exists():
        raise HTTPException(status_code=404, detail="Hotel not found")

    categories = {}
    for category in os.listdir(hotel_path):
        category_path = hotel_path / category
        if category_path.is_dir():
            files = [
                f for f in os.listdir(category_path)
                if os.path.isfile(category_path / f)
            ]
            categories[category] = files

    return categories
