from fastapi import APIRouter, HTTPException, Request
import json
import boto3
from datetime import datetime
from typing import List, Dict, Any

router = APIRouter()

s3 = boto3.client("s3")
BUCKET_NAME = "jmk-project-uploads"
APPROVAL_LOG_KEY = "logs/compliance-history/approval_log.json"
AUDIT_TRAIL_KEY = "logs/compliance-history/audit_trail.json"  # New persistent audit trail


def _get_history_key(hotel_id: str) -> str:
    return f"logs/compliance-history/{hotel_id}.json"


def load_compliance_history(hotel_id: str) -> dict:
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=_get_history_key(hotel_id))
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load history: {e}")


def save_compliance_history(hotel_id: str, history: dict):
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=_get_history_key(hotel_id),
        Body=json.dumps(history, indent=2),
        ContentType="application/json"
    )


def load_audit_trail() -> List[Dict[str, Any]]:
    """Load the permanent audit trail that includes all files with their status"""
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=AUDIT_TRAIL_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load audit trail: {e}")


def save_audit_trail(trail: List[Dict[str, Any]]):
    """Save the permanent audit trail"""
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=AUDIT_TRAIL_KEY,
        Body=json.dumps(trail, indent=2, default=str),
        ContentType="application/json"
    )


def update_audit_trail(hotel_id: str, task_id: str, timestamp: str, action: str, reason: str = None, performed_by: str = "admin"):
    """Update the audit trail with approval/rejection status"""
    trail = load_audit_trail()
    
    # Find the entry to update
    for entry in trail:
        if (entry.get("hotel_id") == hotel_id and 
            entry.get("task_id") == task_id and 
            entry.get("uploaded_at") == timestamp):
            
            # Update the entry with audit information
            entry["status"] = action  # 'approved', 'rejected', 'pending'
            entry["reviewed_at"] = datetime.now().isoformat()
            entry["reviewed_by"] = performed_by
            entry["approved"] = (action == "approved")
            
            if reason:
                entry["rejection_reason"] = reason
            
            save_audit_trail(trail)
            return entry
    
    # If entry not found in audit trail, it might be a new upload
    # This shouldn't happen in normal flow, but let's handle it
    raise HTTPException(status_code=404, detail="Entry not found in audit trail")


def sync_pending_uploads_to_audit_trail():
    """Sync pending uploads from approval_log to audit_trail"""
    try:
        # Load current approval log (pending items)
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        approval_log = json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        approval_log = []
    
    # Load current audit trail
    audit_trail = load_audit_trail()
    
    # Create a set of existing entries for quick lookup
    existing_entries = set()
    for entry in audit_trail:
        key = f"{entry.get('hotel_id')}_{entry.get('task_id')}_{entry.get('uploaded_at')}"
        existing_entries.add(key)
    
    # Add new pending entries to audit trail
    new_entries = []
    for pending_entry in approval_log:
        key = f"{pending_entry.get('hotel_id')}_{pending_entry.get('task_id')}_{pending_entry.get('uploaded_at')}"
        
        if key not in existing_entries:
            # Add to audit trail with pending status
            audit_entry = {
                **pending_entry,
                "status": "pending",
                "approved": False,
                "type": "upload"
            }
            new_entries.append(audit_entry)
    
    if new_entries:
        audit_trail.extend(new_entries)
        save_audit_trail(audit_trail)
    
    return len(new_entries)


@router.get("/history/approval-log")
async def get_approval_log():
    """Get all audit entries (pending, approved, rejected) from the audit trail - FOR AUDIT PAGE"""
    # First sync any new pending uploads
    sync_pending_uploads_to_audit_trail()
    
    # Return the complete audit trail for the audit page
    trail = load_audit_trail()
    return {"entries": trail}


@router.get("/history/pending")
async def get_pending_approvals():
    """Get only pending audit entries - FOR ADMIN DASHBOARD"""
    # First sync any new pending uploads
    sync_pending_uploads_to_audit_trail()
    
    # Return only pending entries for admin dashboard
    trail = load_audit_trail()
    pending_entries = [entry for entry in trail if entry.get("status") == "pending"]
    
    return {"entries": pending_entries}


@router.post("/history/approve")
async def approve_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")
    performed_by = data.get("performed_by", "admin")  # Get from auth context in real app

    # Update the individual hotel history file
    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    found = False

    for entry in entries:
        if entry.get("uploaded_at") == timestamp:
            entry["approved"] = True
            entry["reviewed_at"] = datetime.now().isoformat()
            entry["reviewed_by"] = performed_by
            found = True
            break

    if not found:
        raise HTTPException(status_code=404, detail="Entry not found in hotel history")

    # Save updated hotel history
    save_compliance_history(hotel_id, history)
    
    # Update the audit trail (this maintains the permanent record)
    updated_entry = update_audit_trail(hotel_id, task_id, timestamp, "approved", performed_by=performed_by)
    
    # Remove from pending approval log since it's now processed
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        approval_log = json.loads(obj["Body"].read().decode("utf-8"))
        
        # Remove the processed entry
        approval_log = [e for e in approval_log if not (
            e.get("hotel_id") == hotel_id and
            e.get("task_id") == task_id and
            e.get("uploaded_at") == timestamp
        )]
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=APPROVAL_LOG_KEY,
            Body=json.dumps(approval_log, indent=2),
            ContentType="application/json"
        )
    except s3.exceptions.NoSuchKey:
        pass  # No approval log exists
    
    return {"success": True, "message": "Approved", "entry": updated_entry}


@router.post("/history/reject")
async def reject_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")
    reason = data.get("reason", "No reason given")
    performed_by = data.get("performed_by", "admin")

    # Update the audit trail (this maintains the permanent record)
    updated_entry = update_audit_trail(hotel_id, task_id, timestamp, "rejected", reason=reason, performed_by=performed_by)
    
    # Optionally remove from hotel history (or mark as rejected)
    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    
    # Option 1: Remove rejected entries from hotel history
    # filtered = [e for e in entries if e.get("uploaded_at") != timestamp]
    # history[task_id] = filtered
    
    # Option 2: Mark as rejected in hotel history (recommended)
    for entry in entries:
        if entry.get("uploaded_at") == timestamp:
            entry["approved"] = False
            entry["rejected"] = True
            entry["rejection_reason"] = reason
            entry["reviewed_at"] = datetime.now().isoformat()
            entry["reviewed_by"] = performed_by
            break
    
    save_compliance_history(hotel_id, history)
    
    # Remove from pending approval log
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        approval_log = json.loads(obj["Body"].read().decode("utf-8"))
        
        approval_log = [e for e in approval_log if not (
            e.get("hotel_id") == hotel_id and
            e.get("task_id") == task_id and
            e.get("uploaded_at") == timestamp
        )]
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=APPROVAL_LOG_KEY,
            Body=json.dumps(approval_log, indent=2),
            ContentType="application/json"
        )
    except s3.exceptions.NoSuchKey:
        pass

    return {"success": True, "message": f"Rejected: {reason}", "entry": updated_entry}


@router.delete("/history/delete")
async def delete_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")
    performed_by = data.get("performed_by", "admin")

    # Remove from hotel history
    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    filtered = [e for e in entries if e.get("uploaded_at") != timestamp]

    if len(filtered) == len(entries):
        raise HTTPException(status_code=404, detail="Entry not found")

    history[task_id] = filtered
    save_compliance_history(hotel_id, history)
    
    # Mark as deleted in audit trail (or remove completely)
    trail = load_audit_trail()
    trail = [e for e in trail if not (
        e.get("hotel_id") == hotel_id and
        e.get("task_id") == task_id and
        e.get("uploaded_at") == timestamp
    )]
    save_audit_trail(trail)
    
    # Remove from pending approval log
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=APPROVAL_LOG_KEY)
        approval_log = json.loads(obj["Body"].read().decode("utf-8"))
        
        approval_log = [e for e in approval_log if not (
            e.get("hotel_id") == hotel_id and
            e.get("task_id") == task_id and
            e.get("uploaded_at") == timestamp
        )]
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=APPROVAL_LOG_KEY,
            Body=json.dumps(approval_log, indent=2),
            ContentType="application/json"
        )
    except s3.exceptions.NoSuchKey:
        pass

    return {"success": True, "message": "Deleted"}


@router.get("/history/stats")
async def get_audit_stats():
    """Get audit statistics"""
    trail = load_audit_trail()
    
    stats = {
        "total": len(trail),
        "pending": len([e for e in trail if e.get("status") == "pending"]),
        "approved": len([e for e in trail if e.get("status") == "approved"]),
        "rejected": len([e for e in trail if e.get("status") == "rejected"])
    }
    
    return stats
