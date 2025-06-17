# FILE: backend/app/routers/pdf_generator.py
from fastapi import APIRouter, HTTPException, Response, Query
from fastapi.responses import StreamingResponse
import asyncio
from playwright.async_api import async_playwright
import json
import boto3
import os
import io
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
import calendar

load_dotenv()

router = APIRouter()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

def get_hotel_data_complete(hotel_id: str, audit_type: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """Get complete hotel data for PDF generation"""
    try:
        # Get hotel facilities data
        facilities_key = f"hotels/facilities/{hotel_id}.json"
        facilities_obj = s3.get_object(Bucket=BUCKET_NAME, Key=facilities_key)
        facilities_data = json.loads(facilities_obj["Body"].read().decode("utf-8"))
        
        # Get hotel compliance tasks
        tasks_key = f"hotels/facilities/{hotel_id}tasks.json"
        tasks_obj = s3.get_object(Bucket=BUCKET_NAME, Key=tasks_key)
        tasks_data = json.loads(tasks_obj["Body"].read().decode("utf-8"))
        
        # Filter tasks by audit type
        audit_tasks = []
        for section in tasks_data.get("complianceData", []):
            for task in section.get("tasks", []):
                if task.get("audit") == audit_type:
                    audit_tasks.append(task)
        
        # Get confirmation data for each task
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        for task in audit_tasks:
            if task.get("type") == "confirmation":
                task["confirmation_data"] = get_confirmation_data(hotel_id, task["task_id"], start_dt, end_dt, task.get("frequency", "Monthly"))
            elif task.get("type") == "upload":
                task["upload_data"] = get_upload_data(hotel_id, task["task_id"], start_dt, end_dt)
        
        # Get compliance score
        from app.routers.compliance_score import get_compliance_score
        score_data = get_compliance_score(hotel_id)
        
        return {
            "hotel": {
                "id": hotel_id,
                "name": facilities_data.get("hotelName", "") or facilities_data.get("address", f"Hotel {hotel_id}"),
                "address": facilities_data.get("address", ""),
                "city": facilities_data.get("city", ""),
                "postCode": facilities_data.get("postCode", ""),
                "phone": facilities_data.get("phone", ""),
                "manager": {
                    "name": facilities_data.get("managerName", ""),
                    "phone": facilities_data.get("managerPhone", "") or facilities_data.get("phone", ""),
                    "email": facilities_data.get("managerEmail", "")
                },
                "details": {
                    "sqm": str(facilities_data.get("structural", {}).get("totalSquareMetres", "")),
                    "rooms": str(facilities_data.get("structural", {}).get("totalRooms", "")),
                    "floors": str(facilities_data.get("structural", {}).get("floors", ""))
                }
            },
            "compliance": {
                "score": score_data,
                "tasks": audit_tasks,
                "taskLabels": {task["task_id"]: task["label"] for task in audit_tasks}
            },
            "auditType": audit_type,
            "dateRange": {
                "start": start_date,
                "end": end_date
            },
            "generatedAt": datetime.utcnow().isoformat(),
            "hasIncompleteData": check_incomplete_data(audit_tasks, start_dt, end_dt)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting hotel data: {str(e)}")

def get_confirmation_data(hotel_id: str, task_id: str, start_date: datetime, end_date: datetime, frequency: str) -> Dict[str, Any]:
    """Get confirmation data for a specific task and date range"""
    confirmations = {}
    
    try:
        prefix = f"{hotel_id}/compliance/confirmations/{task_id}/"
        resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".json"):
                meta = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
                conf_data = json.loads(meta["Body"].read().decode("utf-8"))
                
                confirmed_at = conf_data.get("confirmed_at") or conf_data.get("report_date")
                if confirmed_at:
                    conf_date = datetime.strptime(confirmed_at[:10], "%Y-%m-%d")
                    if start_date <= conf_date <= end_date:
                        date_key = conf_date.strftime("%Y-%m-%d")
                        confirmations[date_key] = {
                            "confirmed_by": conf_data.get("confirmed_by", "").split("@")[0],  # Get name part of email
                            "confirmed_at": confirmed_at,
                            "date": conf_date.strftime("%d/%m")
                        }
    except Exception as e:
        print(f"Error getting confirmation data for {task_id}: {str(e)}")
    
    return {
        "frequency": frequency,
        "confirmations": confirmations,
        "grid_data": generate_grid_data(confirmations, frequency, start_date, end_date)
    }

def get_upload_data(hotel_id: str, task_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """Get upload data for a specific task and date range"""
    documents = []
    
    try:
        prefix = f"{hotel_id}/compliance/{task_id}/"
        resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".json"):
                meta = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
                doc_data = json.loads(meta["Body"].read().decode("utf-8"))
                
                report_date_str = doc_data.get("report_date")
                if report_date_str:
                    report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
                    if start_date <= report_date <= end_date:
                        documents.append({
                            "filename": doc_data.get("filename", ""),
                            "report_date": report_date_str,
                            "uploaded_at": doc_data.get("uploaded_at", ""),
                            "fileUrl": doc_data.get("fileUrl", ""),
                            "approved": doc_data.get("approved", False),
                            "uploaded_by": doc_data.get("uploaded_by", "")
                        })
    except Exception as e:
        print(f"Error getting upload data for {task_id}: {str(e)}")
    
    return sorted(documents, key=lambda x: x.get("uploaded_at", ""), reverse=True)

def generate_grid_data(confirmations: Dict[str, Any], frequency: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
    """Generate grid data for different frequencies"""
    today = datetime.now().date()
    
    if frequency.lower() == "daily":
        return generate_daily_grid(confirmations, start_date, end_date, today)
    elif frequency.lower() == "weekly":
        return generate_weekly_grid(confirmations, start_date, end_date, today)
    elif frequency.lower() == "monthly":
        return generate_monthly_grid(confirmations, start_date, end_date, today)
    else:
        return {"boxes": [], "layout": {"cols": 1, "rows": 1}}

def generate_daily_grid(confirmations: Dict[str, Any], start_date: datetime, end_date: datetime, today: date) -> Dict[str, Any]:
    """Generate daily confirmation grid (calendar style) - Always shows all boxes"""
    boxes = []
    current = start_date.date()
    
    while current <= end_date.date():
        date_key = current.strftime("%Y-%m-%d")
        box_data = {
            "date": current.strftime("%d"),
            "month": current.strftime("%b"),
            "full_date": date_key,
            "status": "empty"  # Default to empty, not pending
        }
        
        if date_key in confirmations:
            box_data.update({
                "status": "completed",
                "initials": confirmations[date_key]["confirmed_by"][:2].upper(),
                "confirmed_date": confirmations[date_key]["date"]
            })
        elif current > today:
            box_data["status"] = "future"
        elif current <= today:
            box_data["status"] = "empty"  # Show as empty box to be filled
            
        boxes.append(box_data)
        current += timedelta(days=1)
    
    return {
        "boxes": boxes,
        "layout": {"cols": 31, "rows": 12},  # Calendar style
        "total_expected": len(boxes),
        "completed_count": len([b for b in boxes if b["status"] == "completed"])
    }

def generate_weekly_grid(confirmations: Dict[str, Any], start_date: datetime, end_date: datetime, today: date) -> Dict[str, Any]:
    """Generate weekly confirmation grid - Always shows all expected weeks"""
    boxes = []
    current = start_date.date()
    week_num = 1
    
    while current <= end_date.date():
        # Find confirmations for this week
        week_start = current - timedelta(days=current.weekday())
        week_end = week_start + timedelta(days=6)
        
        week_confirmations = [
            conf for date_str, conf in confirmations.items()
            if week_start <= datetime.strptime(date_str, "%Y-%m-%d").date() <= week_end
        ]
        
        box_data = {
            "week": f"W{week_num}",
            "date_range": f"{week_start.strftime('%d/%m')}-{week_end.strftime('%d/%m')}",
            "status": "empty"  # Default to empty
        }
        
        if week_confirmations:
            latest_conf = max(week_confirmations, key=lambda x: x["confirmed_at"])
            box_data.update({
                "status": "completed",
                "initials": latest_conf["confirmed_by"][:2].upper(),
                "confirmed_date": latest_conf["date"]
            })
        elif week_end > today:
            box_data["status"] = "future"
        else:
            box_data["status"] = "empty"  # Show empty box to be filled
            
        boxes.append(box_data)
        current = week_end + timedelta(days=1)
        week_num += 1
    
    return {
        "boxes": boxes,
        "layout": {"cols": 13, "rows": 4},
        "total_expected": len(boxes),
        "completed_count": len([b for b in boxes if b["status"] == "completed"])
    }

def generate_monthly_grid(confirmations: Dict[str, Any], start_date: datetime, end_date: datetime, today: date) -> Dict[str, Any]:
    """Generate monthly confirmation grid - Always shows all expected months"""
    boxes = []
    current = start_date.replace(day=1)
    
    while current <= end_date:
        month_key = current.strftime("%Y-%m")
        month_confirmations = [
            conf for date_str, conf in confirmations.items()
            if date_str.startswith(month_key)
        ]
        
        box_data = {
            "month": current.strftime("%b"),
            "year": current.strftime("%Y"),
            "month_year": current.strftime("%b %Y"),
            "status": "empty"  # Default to empty
        }
        
        if month_confirmations:
            latest_conf = max(month_confirmations, key=lambda x: x["confirmed_at"])
            box_data.update({
                "status": "completed",
                "initials": latest_conf["confirmed_by"][:2].upper(),
                "confirmed_date": latest_conf["date"]
            })
        elif current.date() > today.replace(day=1):
            box_data["status"] = "future"
        else:
            box_data["status"] = "empty"  # Show empty box to be filled
            
        boxes.append(box_data)
        
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return {
        "boxes": boxes,
        "layout": {"cols": 12, "rows": 1},
        "total_expected": len(boxes),
        "completed_count": len([b for b in boxes if b["status"] == "completed"])
    }

def check_incomplete_data(tasks: List[Dict], start_date: datetime, end_date: datetime) -> bool:
    """Check if there's any incomplete confirmation data"""
    today = datetime.now().date()
    
    for task in tasks:
        if task.get("type") == "confirmation":
            conf_data = task.get("confirmation_data", {})
            grid_data = conf_data.get("grid_data", {})
            
            for box in grid_data.get("boxes", []):
                if box.get("status") == "missing":
                    return True
    return False

@router.post("/audit-report/{hotel_id}/generate-pdf")
async def generate_audit_pdf(
    hotel_id: str,
    audit_type: str = Query(..., description="Type of audit (fire, electrical, gas, legionella, fog, lift, health_safety)"),
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    include_documents: bool = Query(True, description="Include uploaded compliance documents")
):
    """Generate complete audit PDF with real data"""
    
    try:
        # Validate dates
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    # Get all data
    audit_data = get_hotel_data_complete(hotel_id, audit_type, start_date, end_date)
    
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Navigate to your React app's audit page
            await page.goto(f"{FRONTEND_URL}/audit-pdf")
            
            # Wait for the page to load
            await page.wait_for_selector('body')
            
            # Inject the audit data into the page
            await page.evaluate(f"""
                window.auditData = {json.dumps(audit_data)};
                window.dispatchEvent(new CustomEvent('auditDataReady'));
            """)
            
            # Wait for the component to render with data
            await page.wait_for_timeout(3000)
            
            # Generate PDF
            pdf_buffer = await page.pdf(
                format='A4',
                print_background=True,
                margin={
                    'top': '15mm',
                    'bottom': '15mm',
                    'left': '15mm',
                    'right': '15mm'
                }
            )
            
            await browser.close()
            
            # Create filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{hotel_id}_{audit_type}_audit_{timestamp}.pdf"
            
            # Return PDF as download
            return Response(
                content=pdf_buffer,
                media_type='application/pdf',
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"'
                }
            )
            
        except Exception as e:
            if 'browser' in locals():
                await browser.close()
            raise HTTPException(status_code=500, detail=f"PDF generation failed: {str(e)}")

@router.get("/audit-report/{hotel_id}/preview-data")
async def preview_audit_data(
    hotel_id: str,
    audit_type: str = Query(...),
    start_date: str = Query(...),
    end_date: str = Query(...)
):
    """Preview the data that will be used in PDF generation"""
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    audit_data = get_hotel_data_complete(hotel_id, audit_type, start_date, end_date)
    return audit_data
