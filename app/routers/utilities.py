from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse
import base64, requests, os, time

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
):
    try:
        encoded = base64.b64encode(await file.read()).decode()

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
            raise Exception("No documentId returned from DocuPanda.")

        # Get text to detect bill type
        text_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()
        pages_text = text_res.get("result", {}).get("pagesText", [])
        schema_id = SCHEMA_ELECTRICITY if detect_bill_type(pages_text) == "electricity" else SCHEMA_GAS

        # Standardize
        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={"accept": "application/json", "content-type": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        )
        std_id = std_res.json().get("standardizationId")
        if not std_id:
            raise Exception("No standardizationId returned.")

        # Poll result
        for _ in range(6):
            time.sleep(5)
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()

            if result.get("status") == "completed":
                return {"status": "success", "data": result.get("result", {})}

        raise Exception("Standardization timed out.")

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"DocuPanda parse failed: {str(e)}"})
