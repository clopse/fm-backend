from fastapi import APIRouter, UploadFile, File, Form, Depends, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime
import base64
import requests
import os
import time
import boto3

from app.db.session import get_db
from app.db.crud import save_parsed_data_to_db
from app.utils.s3 import save_json_to_s3

router = APIRouter()

DOCUPANDA_API_KEY = os.getenv("DOCUPANDA_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# Detect bill type based on parsed text
def detect_bill_type(pages_text: list[str]) -> str:
    joined = " ".join(pages_text).lower()
    if "mprn" in joined or "mic" in joined or "day units" in joined:
        return "electricity"
    elif "gprn" in joined or "therms" in joined or "gas usage" in joined:
        return "gas"
    return "electricity"

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
            raise HTTPException(status_code=400, detail="Upload to DocuPanda failed")

        document_id = upload_res.json().get("documentId")
        if not document_id:
            raise HTTPException(status_code=400, detail="Missing documentId in response")

        for attempt in range(10):
            doc_check = requests.get(
                f"https://app.docupanda.io/document/{document_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            status = doc_check.get("status")
            print(f"📄 Precheck document status attempt {attempt+1}: {status}")
            if status == "ready":
                break
            time.sleep(6)
        else:
            raise HTTPException(status_code=408, detail="DocuPanda document not ready")

        pages_text = doc_check.get("result", {}).get("pagesText", [])
        bill_type = detect_bill_type(pages_text)
        return {"bill_type": bill_type, "filename": file.filename}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Precheck error: {str(e)}")

@router.post("/utilities/parse-and-save")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    supplier: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    content = await file.read()
    filename = file.filename

    background_tasks.add_task(
        process_and_store_docupanda,
        db, content, hotel_id, utility_type, supplier, filename
    )
    return {"status": "processing", "message": "Upload received. Processing in background."}

def process_and_store_docupanda(db, content, hotel_id, utility_type, supplier, filename):
    try:
        encoded = base64.b64encode(content).decode()

        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )

        print(f"📤 Upload response: {upload_res.status_code} - {upload_res.text}")
        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        if not document_id or not job_id:
            print("❌ Missing documentId or jobId.")
            return

        for attempt in range(10):
            time.sleep(6)
            res = requests.get(
                f"https://app.docupanda.io/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            status = res.get("status")
            print(f"🕓 Upload job status: {status}")
            if status == "completed":
                break
            if status == "error":
                print("❌ Upload job failed.")
                return
        else:
            print("❌ Upload job timeout.")
            return

        for attempt in range(10):
            time.sleep(10)
            doc_check = requests.get(
                f"https://app.docupanda.io/document/{document_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            doc_status = doc_check.get("status")
            print(f"📄 Document status: {doc_status}")
            if doc_status == "ready":
                break
        else:
            print("❌ Document never reached 'ready' status.")
            return

        pages_text = doc_check.get("result", {}).get("pagesText", [])
        detected_type = detect_bill_type(pages_text)
        schema_id = SCHEMA_ELECTRICITY if detected_type == "electricity" else SCHEMA_GAS

        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )

        print(f"⚙️ Standardization response: {std_res.status_code} - {std_res.text}")
        std_data = std_res.json()
        std_id = std_data.get("standardizationId")
        if not std_id:
            print("❌ No standardizationId returned.")
            return

        for attempt in range(10):
            time.sleep(6)
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            if result.get("status") == "completed":
                parsed = result.get("result", {})
                billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
                s3_path = save_json_to_s3(parsed, hotel_id, detected_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, detected_type, parsed, s3_path)
                print(f"✅ Bill parsed and saved: {s3_path}")
                return
            elif result.get("status") == "error":
                print(f"❌ Standardization error: {result}")
                return
        else:
            print("❌ Standardization polling timed out.")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

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
        prefix = f"{hotel_id}/utilities/{year}/"
        result = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)
        files = [obj["Key"] for obj in result.get("Contents", [])]
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list files: {str(e)}")
