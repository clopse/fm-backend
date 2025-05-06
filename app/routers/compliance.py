 from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
 from sqlalchemy.orm import Session
 from datetime import datetime, timedelta
 from dotenv import load_dotenv
 import os
 import json
 import boto3
 from typing import List
 
 from app.database import get_db
 from app.models import SafetyCheck
 from app.schemas.common import SuccessResponse
 from app.schemas.safety import SafetyScoreResponse, WeeklyScore
 
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
     file: UploadFile = File(...),
     db: Session = Depends(get_db)
 ):
     if not file or not file.file:
         raise HTTPException(status_code=400, detail="No file received")
 
     timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
     s3_key = f"{hotel_id}/compliance/{task_id}/{timestamp}_{file.filename}"
     metadata_key = s3_key + ".json"
 
     try:
         # Upload the file
         s3.upload_fileobj(file.file, os.getenv("AWS_BUCKET_NAME"), s3_key)
 
         # Create metadata JSON
         metadata = {
             "report_date": report_date,
             "uploaded_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
             "filename": file.filename,
         }
 
         # Upload metadata as JSON file
         s3.put_object(
             Body=json.dumps(metadata, indent=2),
             Bucket=os.getenv("AWS_BUCKET_NAME"),
             Key=metadata_key,
         )
 
     except Exception as e:
         raise HTTPException(status_code=500, detail=f"Error uploading to S3: {str(e)}")
 
     # Record in database if needed
     db_doc = SafetyCheck(
         hotel_id=hotel_id,
         task_id=task_id,
         file_path=s3_key,
         uploaded_at=datetime.utcnow(),
     )
     db.add(db_doc)
     db.commit()
 
     return SuccessResponse(id=str(db_doc.id), message="Compliance file uploaded successfully")
 
 
 @router.get("/api/compliance/score/{hotel_id}", response_model=SafetyScoreResponse)
 def get_compliance_score(hotel_id: str):
     RULES_PATH = "app/data/taskRules.json"
     now = datetime.now()
     three_months_ago = now - timedelta(days=90)
     reports_path = f"{hotel_id}/compliance"
 
     try:
         with open(RULES_PATH, "r") as f:
             task_rules = json.load(f)
     except Exception as e:
         raise HTTPException(status_code=500, detail=f"Could not load task rules: {e}")
 
     total_points = 0
     earned_points = 0
     breakdown = {}
 
     for task_id, rule in task_rules.items():
         rule_points = rule.get("points", 10)
         total_points += rule_points
         task_score = 0
 
         try:
             resp = s3.list_objects_v2(
                 Bucket=os.getenv("AWS_BUCKET_NAME"),
                 Prefix=f"{reports_path}/{task_id}/"
             )
             for obj in resp.get("Contents", []):
                 if obj["Key"].endswith(".json"):
                     file_metadata = s3.get_object(Bucket=os.getenv("AWS_BUCKET_NAME"), Key=obj["Key"])
                     data = json.loads(file_metadata["Body"].read().decode("utf-8"))
                     report_date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                     if report_date >= three_months_ago:
                         task_score = rule_points
                         break
         except Exception:
             continue
 
         breakdown[task_id] = task_score
         earned_points += task_score
 
     return SafetyScoreResponse(
         total_points=total_points,
         earned_points=earned_points,
         score_percent=round((earned_points / total_points) * 100) if total_points else 0,
         breakdown=breakdown
     )
 
 
 @router.get("/api/compliance/score-history/{hotel_id}", response_model=List[WeeklyScore])
 def get_compliance_score_history(hotel_id: str):
     RULES_PATH = "app/data/taskRules.json"
     reports_path = f"{hotel_id}/compliance"
 
     try:
         with open(RULES_PATH, "r") as f:
             task_rules = json.load(f)
     except Exception as e:
         raise HTTPException(status_code=500, detail=f"Could not load task rules: {e}")
 
     history = {week: {"earned": 0, "total": 0} for week in range(1, 54)}
 
     for task_id, rule in task_rules.items():
         rule_points = rule.get("points", 10)
 
         try:
             resp = s3.list_objects_v2(
                 Bucket=os.getenv("AWS_BUCKET_NAME"),
                 Prefix=f"{reports_path}/{task_id}/"
             )
             for obj in resp.get("Contents", []):
                 if obj["Key"].endswith(".json"):
                     file_metadata = s3.get_object(Bucket=os.getenv("AWS_BUCKET_NAME"), Key=obj["Key"])
                     data = json.loads(file_metadata["Body"].read().decode("utf-8"))
                     report_date = datetime.strptime(data["report_date"], "%Y-%m-%d")
                     week = int(report_date.strftime("%W"))
                     history[week]["earned"] += rule_points
                     history[week]["total"] += rule_points
         except Exception:
             continue
 
     return [
         WeeklyScore(week=week, score=round((data["earned"] / data["total"]) * 100, 1))
         for week, data in history.items() if data["total"] > 0
     ]
