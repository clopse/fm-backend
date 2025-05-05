from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import time
import os
import boto3

from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3

router = APIRouter()

# Schema IDs
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"
DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")

# -------- Utility Functions -------- #

def detect_bill_type(pages_text: list[str]) -> str:
    joined = " ".join(pages_text).lower()
    if "mprn" in joined or "mic" in joined or "day units" in joined:
        return "electricity"
    elif "gprn" in joined or "therms" in joined or "gas usage" in joined:
        return "gas"
    return "unknown"

def poll_job_status(job_id: str, max_attempts: int = 20, delay: int = 6) -> bool:
    for attempt in range(max_attempts):
        res = requests.get(
            f"https://app.docupanda.io/job/{job_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        )
        status = res.json().get("status")
        print(f"🕓 Upload job status ({attempt + 1}): {status}")
        if status == "completed":
            return True
        if status == "error":
            raise RuntimeError("Upload job failed.")
        time.sleep(delay)
    raise RuntimeError("Upload job timed out.")

def wait_for_document_ready(document_id: str, max_attempts: int = 10, delay: int = 15):
    for attempt in range(max_attempts):
        res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        )
        status = res.json().get("status")
        print(f"📄 Doc status check ({attempt + 1}): {status}")
        if status == "ready":
            return True
        time.sleep(delay)
    raise RuntimeError("Document not ready after retries.")

# -------- Routes -------- #

@router.post("/utilities/precheck")
async def precheck_bill_type(file: UploadFile = File(...)):
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

        if upload_res.status_code != 200:
            raise HTTPException(status_code=400, detail="Upload failed")

        document_id = upload_res.json().get("documentId")
        if not document_id:
            raise HTTPException(status_code=400, detail="No documentId returned")

        doc_res = requests.get(
            f"https://app.docupanda.io/document/{document_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()

        pages_text = doc_res.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)

        return {"bill_type": bill_type, "filename": file.filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/utilities/parse-and-save")
async def parse_and_save(
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    supplier: str = Form(...),
    file: UploadFile = File(...)
):
    try:
        content = await file.read()
        encoded = base64.b64encode(content).decode()

        # Upload to DocuPanda
        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": file.filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        if upload_res.status_code != 200:
            raise HTTPException(status_code=400, detail="Upload to DocuPanda failed")

        doc_data = upload_res.json()
        document_id = doc_data.get("documentId")
        upload_job_id = doc_data.get("jobId")
        if not document_id or not upload_job_id:
            raise HTTPException(status_code=400, detail="Missing documentId or jobId")

        # Wait for upload to complete
        poll_job_status(upload_job_id)

        # Wait for document to be marked ready
        wait_for_document_ready(document_id)

        # Get schema
        bill_type = utility_type.lower()
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else (
            SCHEMA_GAS if bill_type == "gas" else None
        )
        if not schema_id:
            raise HTTPException(status_code=400, detail="Unknown utility type")

        # Trigger standardization
        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        if std_res.status_code != 200:
            raise HTTPException(status_code=400, detail="Standardization request failed")

        std_data = std_res.json()
        standardization_id = std_data.get("standardizationId")
        std_job_id = std_data.get("jobId")

        if not standardization_id or not std_job_id:
            raise HTTPException(status_code=500, detail="Missing standardizationId or jobId")

        return {
            "status": "processing",
            "document_id": document_id,
            "upload_job_id": upload_job_id,
            "standardization_id": standardization_id,
            "standardization_job_id": std_job_id,
            "filename": file.filename,
            "bill_type": bill_type,
        }

    except Exception as e:
        print(f"❌ DocuPanda error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"DocuPanda parse error: {str(e)}")


@router.post("/utilities/finalize")
async def finalize_parsed_bill(
    document_id: str = Form(...),
    standardization_id: str = Form(...),
    hotel_id: str = Form(...),
    bill_type: str = Form(...),
    filename: str = Form(...),
    db: Session = Depends(get_db)
):
    try:
        std_status = requests.get(
            f"https://app.docupanda.io/standardize/{standardization_id}",
            headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
        ).json()

        if std_status.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Standardization not yet complete")

        parsed = std_status.get("result", {})
        billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
        s3_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
        save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_path)

        return {"status": "saved", "path": s3_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Finalize error: {str(e)}")


@router.get("/api/utilities/{hotel_id}/{year}")
def list_uploaded_utilities(hotel_id: str, year: str):
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "eu-west-1")
        )
        bucket = os.getenv("AWS_BUCKET_NAME")
        prefix = f"{hotel_id}/utilities/{year}/"

        result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [obj["Key"] for obj in result.get("Contents", [])]

        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")
