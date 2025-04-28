from fastapi import APIRouter, HTTPException
from typing import Dict, List
from app.services.storage_service import list_files, generate_signed_url  # ✅ Use your signed URL function

router = APIRouter()

@router.get("/files/{hotel_id}")
async def list_service_files(hotel_id: str) -> Dict[str, Dict[str, List[Dict[str, str]]]]:
    try:
        prefix = f"{hotel_id}/"
        objects = list_files(prefix)  # ✅ Load object keys using your working `list_files`

        if not objects:
            return {}

        reports = {
            "Service Reports": {},
            "Contracts": {}
        }

        for obj in objects:
            key = obj["Key"]

            if key.endswith("/"):
                continue

            parts = key.split("/", 3)
            if len(parts) < 4:
                continue

            _, top_folder, company_folder, filename = parts

            if top_folder == "reports":
                section = "Service Reports"
            elif top_folder == "contracts":
                section = "Contracts"
            else:
                continue

            if company_folder not in reports[section]:
                reports[section][company_folder] = []

            # ✅ Instead of building a public URL, generate a signed one:
            signed_url = generate_signed_url(key, expires_in=3600)

            reports[section][company_folder].append({
                "filename": filename,
                "url": signed_url,
            })

        return reports

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")
