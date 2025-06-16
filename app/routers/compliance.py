from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import boto3
from app.routers.compliance_history import add_history_entry
from .compliance_score import get_compliance_score

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET = os.getenv("AWS_BUCKET_NAME")
router = APIRouter()

@router.post("/uploads/compliance")
async def upload_compliance_doc(
    hotel_id: str = Form(...),
    task_id: str = Form(...),
    report_date: str = Form(...),
    file: UploadFile = File(...)
):
    if not file or not file.file:
        raise HTTPException(status_code=400, detail="No file received")

    try:
        parsed_date = datetime.strptime(report_date, "%Y-%m-%d")
        if parsed_date > datetime.utcnow():
            raise HTTPException(status_code=400, detail="Report date cannot be in the future.")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid report_date format. Use YYYY-MM-DD.")

    timestamp = datetime.utcnow()
    report_tag = parsed_date.strftime('%Y%m%d')
    unique_suffix = timestamp.strftime('%H%M%S')
    safe_filename = file.filename.replace(" ", "_")
    s3_key = f"{hotel_id}/compliance/{task_id}/{report_tag}_{unique_suffix}_{safe_filename}"
    metadata_key = s3_key + ".json"
    file_url = f"https://{BUCKET}.s3.amazonaws.com/{s3_key}"

    metadata = {
        "report_date": parsed_date.strftime("%Y-%m-%d"),
        "uploaded_at": timestamp.isoformat(),
        "filename": safe_filename,
        "fileUrl": file_url,
        "uploaded_by": "SYSTEM",
        "approved": False,
        "type": "upload"
    }

    try:
        s3.upload_fileobj(
            Fileobj=file.file,
            Bucket=BUCKET,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/pdf",
                "ContentDisposition": "inline"
            }
        )
        s3.put_object(Bucket=BUCKET, Key=metadata_key, Body=json.dumps(metadata, indent=2))
        add_history_entry(hotel_id, task_id, metadata)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload error: {str(e)}")

    return {"message": "Upload successful", "file": file_url}

@router.get("/compliance/documents/{hotel_id}")
async def get_compliance_documents(hotel_id: str):
    """Get all compliance documents for a hotel"""
    try:
        documents = []
        prefix = f"{hotel_id}/compliance/"
        
        # List all objects in the hotel's compliance folder
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        
        for obj in response.get('Contents', []):
            # Only process metadata files, skip the actual PDFs and other files
            if obj['Key'].endswith('.json') and not obj['Key'].endswith('latest.json') and not obj['Key'].endswith('tasks.json') and not obj['Key'].endswith('details.json'):
                try:
                    # Get document metadata
                    metadata_obj = s3.get_object(Bucket=BUCKET, Key=obj['Key'])
                    metadata = json.loads(metadata_obj['Body'].read().decode('utf-8'))
                    
                    # Extract task_id from the S3 path
                    # Path format: {hotel_id}/compliance/{task_id}/{filename}.json
                    # or {hotel_id}/compliance/confirmations/{task_id}/{filename}.json
                    path_parts = obj['Key'].split('/')
                    
                    if 'confirmations' in path_parts:
                        # Handle confirmation files: hotel_id/compliance/confirmations/task_id/file.json
                        task_id = path_parts[3] if len(path_parts) > 3 else 'unknown'
                    else:
                        # Handle regular compliance files: hotel_id/compliance/task_id/file.json
                        task_id = path_parts[2] if len(path_parts) > 2 else 'unknown'
                    
                    # Only include files with valid metadata structure
                    if metadata.get('type') in ['upload', 'confirmation']:
                        documents.append({
                            'task_id': task_id,
                            'filename': metadata.get('filename', 'Unknown'),
                            'fileUrl': metadata.get('fileUrl', ''),
                            'report_date': metadata.get('report_date', ''),
                            'uploaded_at': metadata.get('uploaded_at', metadata.get('confirmed_at', '')),
                            'uploaded_by': metadata.get('uploaded_by', metadata.get('confirmed_by', '')),
                            'approved': metadata.get('approved', False),
                            'type': metadata.get('type', 'upload')
                        })
                        
                except Exception as e:
                    print(f"Error processing document {obj['Key']}: {e}")
                    continue
        
        # Sort documents by upload date (newest first)
        documents.sort(key=lambda x: x.get('uploaded_at', ''), reverse=True)
        
        return {"documents": documents}
        
    except Exception as e:
        print(f"Error fetching compliance documents for {hotel_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch documents: {str(e)}")

@router.get("/compliance/task-labels")
def get_task_labels():
    try:
        # Keep using master compliance.json for task labels - we want all possible labels
        with open("app/data/compliance.json") as f:
            data = json.load(f)
        
        label_map = {
            task["task_id"]: task["label"]
            for section in data
            for task in section.get("tasks", [])
        }
        return label_map
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not load task labels: {e}")
