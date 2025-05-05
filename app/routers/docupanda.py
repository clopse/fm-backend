from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
import base64
import requests
import os
import time

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "bd3ec499"

# Detect bill type from pages text
def detect_bill_type(pages_text: list[str]) -> str:
    joined = " ".join(pages_text).lower()
    if "mprn" in joined or "mic" in joined or "day units" in joined:
        return "electricity"
    elif "gprn" in joined or "therms" in joined or "gas usage" in joined:
        return "gas"
    return "electricity"  # default fallback

@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        content = await file.read()
        encoded = base64.b64encode(content).decode()

        # Step 1: Upload document
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

        # Step 2: Get plain text
        doc_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()
        pages_text = doc_res.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS

        # Step 3: Request standardization
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

        # Step 4: Poll for result
        for _ in range(6):
            time.sleep(5)
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()

            if result.get("status") == "completed":
                return result.get("result", {})

        raise Exception("Standardization timed out or failed.")

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"DocuPanda parse failed: {str(e)}"},
        )
