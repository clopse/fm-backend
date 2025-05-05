import base64
import requests
import time
from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse

DOCUPANDA_API_KEY = "YOUR_API_KEY"  # Replace with your actual DocuPanda API key
DOCUPANDA_BASE = "https://app.docupanda.io"
SCHEMA_ID = "YOUR_SCHEMA_ID"  # Replace with your DocuPanda schema ID

router = APIRouter()

@router.post("/utilities/parse-and-save-docupanda")
async def parse_and_save_docupanda(
    file: UploadFile = File(...),
    hotel_id: str = Form(...)
):
    try:
        # 1. Read and encode file
        content = await file.read()
        encoded = base64.b64encode(content).decode()

        # 2. Upload to DocuPanda
        upload_res = requests.post(
            f"{DOCUPANDA_BASE}/document",
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY
            },
            json={"document": {"file": {"contents": encoded, "filename": file.filename}}}
        )

        if upload_res.status_code != 200:
            return JSONResponse(status_code=500, content={"detail": "DocuPanda upload failed."})

        doc_id = upload_res.json().get("documentId")
        if not doc_id:
            return JSONResponse(status_code=500, content={"detail": "No document ID returned from DocuPanda."})

        # 3. Wait for document to be ready
        for _ in range(10):  # wait up to 30 seconds
            doc_res = requests.get(
                f"{DOCUPANDA_BASE}/document/{doc_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY}
            )
            status = doc_res.json().get("status")
            if status == "completed":
                break
            time.sleep(3)
        else:
            return JSONResponse(status_code=500, content={"detail": "DocuPanda processing timeout."})

        # 4. Trigger standardization
        standardize_res = requests.post(
            f"{DOCUPANDA_BASE}/standardize/batch",
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY
            },
            json={"documentIds": [doc_id], "schemaId": SCHEMA_ID}
        )

        std_id = standardize_res.json().get("standardizationId")
        if not std_id:
            return JSONResponse(status_code=500, content={"detail": "Failed to initiate standardization."})

        # 5. Wait for standardization result
        for _ in range(10):
            std_res = requests.get(
                f"{DOCUPANDA_BASE}/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY}
            )
            std_data = std_res.json()
            if std_data.get("status") == "completed" and std_data.get("result"):
                break
            time.sleep(3)
        else:
            return JSONResponse(status_code=500, content={"detail": "DocuPanda standardization timeout."})

        # 6. Return structured fields
        return {
            "hotel_id": hotel_id,
            "parsed": std_data["result"]
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Parse failed: {str(e)}"})
