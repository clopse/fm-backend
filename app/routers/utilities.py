from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import os
import boto3
import pdfplumber
from io import BytesIO

from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

def detect_bill_type_from_pdf(file_bytes: bytes) -> str:
    try:
        with pdfplumber.open(BytesIO(file_bytes)) as pdf:
            all_text = " ".join([page.extract_text() or "" for page in pdf.pages]).lower()
            if "mprn" in all_text or "mic" in all_text or "day units" in all_text:
                return "electricity"
            elif "gprn" in all_text or "therms" in all_text or "gas usage" in all_text:
                return "gas"
    except Exception as e:
        print(f"❌ Error reading PDF: {e}")
    return "electricity"

@router.post("/utilities/precheck")
async def precheck_bill_type(file: UploadFile = File(...)):
    try:
        content = await file.read()
        bill_type = detect_bill_type_from_pdf(content)
        return {"bill_type": bill_type, "filename": file.filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Precheck error: {str(e)}")

@router.post("/utilities/parse-and-save")
async def parse_and_save(
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    supplier: str = Form(...),
    file: UploadFile = File(...),
):
    try:
        content = await file.read()
        filename = file.filename
        encoded = base64.b64encode(content).decode()

        print(f"\n📦 Submitting document {filename} to DocuPanda")

        upload_payload = {
            "document": {"file": {"contents": encoded, "filename": filename}}
        }
        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json=upload_payload,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        print(f"📤 Upload response: {upload_res.status_code}")
        print(upload_res.text)

        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")

        if not document_id or not job_id:
            print("❌ Missing documentId or jobId from upload response")
            raise HTTPException(status_code=500, detail="Missing documentId or jobId")

        schema_id = SCHEMA_ELECTRICITY if utility_type == "electricity" else SCHEMA_GAS
        std_payload = {
            "documentIds": [document_id],
            "schemaId": schema_id
        }
        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json=std_payload,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )
        print(f"⚙️ Standardization request: {std_res.status_code}")
        print(std_res.text)

        std_data = std_res.json()
        standardization_id = std_data.get("standardizationId")
        if not standardization_id:
            print("❌ No standardizationId returned")
            raise HTTPException(status_code=500, detail="Missing standardizationId")

        return {
            "status": "submitted",
            "jobId": job_id,
            "documentId": document_id,
            "standardizationId": standardization_id,
            "filename": filename
        }

    except Exception as e:
        print(f"❌ Error during parse-and-save: {e}")
        raise HTTPException(status_code=500, detail=f"Parse error: {str(e)}")

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

        print(f"📦 Finalizing bill for document {document_id}, std ID {standardization_id}")
        print(std_status)

        if std_status.get("status") != "completed":
            raise HTTPException(status_code=400, detail="Standardization not yet complete")

        parsed = std_status.get("result", {})
        billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
        s3_path = save_json_to_s3(parsed, hotel_id, bill_type, billing_start, filename)
        save_parsed_data_to_db(db, hotel_id, bill_type, parsed, s3_path)

        print(f"✅ Bill parsed and saved to {s3_path}")

        return {"status": "saved", "path": s3_path}
    except Exception as e:
        print(f"❌ Finalize error: {e}")
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
        prefix = f"{hotel_id}/utilities/{year}/"
        result = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)
        files = [obj["Key"] for obj in result.get("Contents", [])]
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")
