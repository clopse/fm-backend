from fastapi import APIRouter, UploadFile, File, Form, Depends
from fastapi.responses import JSONResponse
import base64, requests, os, time
from datetime import datetime
from app.utils.s3 import save_json_to_s3
from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from sqlalchemy.orm import Session

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "bd3ec499"

def detect_bill_type(pages_text: list[str]) -> str:
    joined = " ".join(pages_text).lower()
    if "mprn" in joined or "mic" in joined or "day units" in joined:
        return "electricity"
    elif "gprn" in joined or "therms" in joined or "gas usage" in joined:
        return "gas"
    return "electricity"

@router.post("/utilities/parse-and-save")
async def parse_and_save(
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    supplier: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    try:
        content = await file.read()
        encoded = base64.b64encode(content).decode()

        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": file.filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        document_id = upload_res.json().get("documentId")
        if not document_id:
            raise Exception("No documentId returned from DocuPanda upload.")

        doc_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()
        pages_text = doc_res.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS

        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        std_id = std_res.json().get("standardizationId")
        if not std_id:
            raise Exception("No standardizationId returned.")

        for _ in range(6):
            time.sleep(5)
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()

            if result.get("status") == "completed":
                parsed = result.get("result", {})
                now = datetime.utcnow()
                year = now.strftime("%Y")
                s3_path = f"{hotel_id}/{year}/{utility_type}/{file.filename.replace(' ', '_')}".replace(".pdf", ".json")

                save_json_to_s3(parsed, s3_path)
                save_parsed_data_to_db(db, hotel_id, utility_type, parsed, s3_path)

                return {"status": "success", "data": parsed, "s3_path": s3_path}

        raise Exception("Standardization timed out or failed.")

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"DocuPanda parse failed: {str(e)}"},
        )
