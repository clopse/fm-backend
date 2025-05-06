from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import boto3
from app.schemas.common import SuccessResponse

# Load environment variables
load_dotenv()

# Initialize AWS S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

router = APIRouter()

@router.post("/uploads/compliance", response_model=SuccessResponse)
async def upload_compliance_doc(
    hotel_id: str = Form(...),
    task_id: str = Form(...),
    report_date: str = Form(...),
    file: UploadFile = File(...)
):
    if not file or not file.file:
        raise HTTPException(status_code=400, detail="No file received")

    # Validate and parse report date
    try:
        parsed_date = datetime.strptime(report_date, "%d/%m/%Y")
        if parsed_date > datetime.utcnow():
            raise HTTPException(status_code=400, detail="Report date cannot be in the future.")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid report_date format. Use DD/MM/YYYY.")

    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    s3_key = f"{hotel_id}/compliance/{task_id}/{timestamp}_{file.filename}"
    metadata_key = s3_key + ".json"

    try:
        # Upload file to S3
        s3.upload_fileobj(file.file, os.getenv("AWS_BUCKET_NAME"), s3_key)

        # Upload metadata
        metadata = {
            "report_date": parsed_date.strftime("%Y-%m-%d"),
            "uploaded_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "filename": file.filename,
        }

        s3.put_object(
            Body=json.dumps(metadata, indent=2),
            Bucket=os.getenv("AWS_BUCKET_NAME"),
            Key=metadata_key,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading to S3: {str(e)}")

    return SuccessResponse(id=s3_key, message="Compliance file uploaded successfully")
