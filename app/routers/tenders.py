# app/routers/tenders.py

from fastapi import APIRouter, Query
from app.services.storage_service import list_files, generate_public_url

router = APIRouter(
    prefix="/tenders",
    tags=["Tenders"],
)

@router.get("/list")
def list_tenders(hotel_id: str = Query(...)):
    prefix = f"{hotel_id}/tenders/"
    try:
        files = list_files(prefix)
        result = []

        for file in files:
            key = file["Key"]
            if key.endswith("/"):  # skip folders
                continue
            url = generate_public_url(key)
            filename = key.split("/")[-1]
            result.append({
                "name": filename,
                "url": url,
                "last_modified": file.get("LastModified"),
                "size": file.get("Size"),
            })

        return result

    except Exception as e:
        return {"error": str(e)}
