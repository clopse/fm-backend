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
    """Sync pending uploads from approval_log to audit_trail AND ensure all hotel history files are captured"""
    # Load current approval log (pending items)
    try:
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
    
    # IMPORTANT: Also scan individual hotel history files for approved/rejected entries
    # This captures files that were processed before the audit trail system existed
    try:
        # List all hotel history files
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="logs/compliance-history/"):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Skip if not a hotel history file (avoid approval_log.json and audit_trail.json)
                if not key.endswith('.json') or key.endswith('approval_log.json') or key.endswith('audit_trail.json'):
                    continue
                
                try:
                    # Load the hotel history file
                    hotel_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                    hotel_history = json.loads(hotel_obj["Body"].read().decode("utf-8"))
                    
                    # Extract hotel_id from filename
                    hotel_id = key.split('/')[-1].replace('.json', '')
                    
                    # Process each task's history in this hotel file
                    for task_id, task_entries in hotel_history.items():
                        if isinstance(task_entries, list):
                            for entry in task_entries:
                                entry_key = f"{hotel_id}_{task_id}_{entry.get('uploaded_at')}"
                                
                                if entry_key not in existing_entries and entry.get('uploaded_at'):
                                    # Determine status from the entry
                                    if entry.get('approved'):
                                        status = 'approved'
                                    elif entry.get('rejected') or entry.get('rejection_reason'):
                                        status = 'rejected'
                                    else:
                                        status = 'pending'
                                    
                                    # Add to audit trail
                                    audit_entry = {
                                        "hotel_id": hotel_id,
                                        "task_id": task_id,
                                        "fileUrl": entry.get('fileUrl'),
                                        "filename": entry.get('filename'),
                                        "reportDate": entry.get('reportDate') or entry.get('report_date'),
                                        "uploadedAt": entry.get('uploaded_at'),
                                        "uploaded_by": entry.get('uploaded_by'),
                                        "status": status,
                                        "approved": entry.get('approved', False),
                                        "type": "upload",
                                        "reviewed_at": entry.get('reviewed_at'),
                                        "reviewed_by": entry.get('reviewed_by'),
                                        "rejection_reason": entry.get('rejection_reason')
                                    }
                                    new_entries.append(audit_entry)
                                    existing_entries.add(entry_key)
                
                except Exception as e:
                    print(f"Error processing hotel history file {key}: {e}")
                    continue
    
    except Exception as e:
        print(f"Error scanning hotel history files: {e}")
    
    if new_entries:
        audit_trail.extend(new_entries)
        save_audit_trail(audit_trail)
    
    return len(new_entries)


# NEW ENDPOINT: For admin dashboard - only pending files
@router.get("/history/pending")
async def get_pending_approvals():
    """Get only pending audit entries - FOR ADMIN DASHBOARD"""
    # First sync any new pending uploads
    sync_pending_uploads_to_audit_trail()
    
    # Return only pending entries for admin dashboard
    trail = load_audit_trail()
    pending_entries = [entry for entry in trail if entry.get("status") == "pending"]
    
    return {"entries": pending_entries}


# EXISTING ENDPOINT: For audit page - full history
@router.get("/history/approval-log")
async def get_approval_log():
    """Get all audit entries (pending, approved, rejected) from the audit trail - FOR AUDIT PAGE"""
    # First sync any new pending uploads
    sync_pending_uploads_to_audit_trail()
    
    # Return the complete audit trail for the audit page
    trail = load_audit_trail()
    return {"entries": trail}


@router.post("/history/approve")
async def approve_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")
    performed_by = data.get("performed_by", "admin")

    # STEP 1: Update the individual hotel history file (KEEP EXISTING LOGIC)
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
        raise HTTPException(status_code=404, detail="Entry not found")

    save_compliance_history(hotel_id, history)
    
    # STEP 2: Update the audit trail (FIXED - ensure this actually works)
    try:
        trail = load_audit_trail()
        trail_updated = False
        
        # Find and update the entry in audit trail
        for trail_entry in trail:
            if (trail_entry.get("hotel_id") == hotel_id and 
                trail_entry.get("task_id") == task_id and 
                trail_entry.get("uploadedAt") == timestamp):
                
                trail_entry["status"] = "approved"
                trail_entry["reviewed_at"] = datetime.now().isoformat()
                trail_entry["reviewed_by"] = performed_by
                trail_entry["approved"] = True
                trail_updated = True
                break
        
        # If not found in trail, add it (this handles the case where it wasn't synced yet)
        if not trail_updated:
            # Find the entry from hotel history that we just updated
            approved_entry = None
            for entry in entries:
                if entry.get("uploaded_at") == timestamp:
                    approved_entry = entry
                    break
            
            if approved_entry:
                # Add to audit trail
                new_trail_entry = {
                    "hotel_id": hotel_id,
                    "task_id": task_id,
                    "fileUrl": approved_entry.get('fileUrl'),
                    "filename": approved_entry.get('filename'),
                    "reportDate": approved_entry.get('reportDate') or approved_entry.get('report_date'),
                    "uploadedAt": approved_entry.get('uploaded_at'),
                    "uploaded_by": approved_entry.get('uploaded_by'),
                    "status": "approved",
                    "approved": True,
                    "type": "upload",
                    "reviewed_at": datetime.now().isoformat(),
                    "reviewed_by": performed_by
                }
                trail.append(new_trail_entry)
        
        # Save the updated audit trail
        save_audit_trail(trail)
        
    except Exception as e:
        print(f"Error updating audit trail: {e}")
        # Don't fail the whole operation if audit trail update fails
        pass
    
    # STEP 3: Remove from pending approval log (KEEP EXISTING)
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

    return {"success": True, "message": "Approved"}


@router.post("/history/reject")
async def reject_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")
    reason = data.get("reason", "No reason given")
    performed_by = data.get("performed_by", "admin")

    # KEEP EXISTING LOGIC: Update hotel history
    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    filtered = [e for e in entries if e.get("uploaded_at") != timestamp]

    if len(filtered) == len(entries):
        raise HTTPException(status_code=404, detail="Entry not found")

    history[task_id] = filtered
    save_compliance_history(hotel_id, history)
    
    # NEW: Update audit trail to maintain permanent record
    try:
        update_audit_trail(hotel_id, task_id, timestamp, "rejected", reason=reason, performed_by=performed_by)
    except HTTPException:
        # If not in audit trail yet, that's ok - the old system will still work
        pass
    
    # KEEP EXISTING: Remove from pending approval log
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

    return {"success": True, "message": f"Rejected: {reason}"}


@router.delete("/history/delete")
async def delete_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")

    # KEEP EXISTING LOGIC
    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    filtered = [e for e in entries if e.get("uploaded_at") != timestamp]

    if len(filtered) == len(entries):
        raise HTTPException(status_code=404, detail="Entry not found")

    history[task_id] = filtered
    save_compliance_history(hotel_id, history)
    
    # NEW: Remove from audit trail too
    try:
        trail = load_audit_trail()
        trail = [e for e in trail if not (
            e.get("hotel_id") == hotel_id and
            e.get("task_id") == task_id and
            e.get("uploaded_at") == timestamp
        )]
        save_audit_trail(trail)
    except Exception:
        pass  # If audit trail doesn't exist yet, that's ok
    
    # KEEP EXISTING: Remove from pending approval log
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
