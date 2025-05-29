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


def sync_pending_uploads_to_audit_trail():
    """Sync and deduplicate all entries in audit trail"""
    # Load current audit trail
    audit_trail = load_audit_trail()
    
    # Create a comprehensive deduplication map
    # Key format: hotel_id_task_id_uploaded_at -> latest_entry
    entries_map = {}
    
    # First, process existing audit trail entries
    for entry in audit_trail:
        uploaded_at = entry.get('uploadedAt') or entry.get('uploaded_at')
        key = f"{entry.get('hotel_id')}_{entry.get('task_id')}_{uploaded_at}"
        
        # Keep the most recent or most processed entry (approved/rejected over pending)
        if key not in entries_map:
            entries_map[key] = entry
        else:
            current = entries_map[key]
            current_status = current.get('status', 'pending')
            new_status = entry.get('status', 'pending')
            
            # Priority: approved/rejected > pending
            if current_status == 'pending' and new_status in ['approved', 'rejected']:
                entries_map[key] = entry
            elif current_status == new_status and entry.get('reviewed_at'):
                # If same status, prefer the one with review timestamp
                entries_map[key] = entry
    
    # Now scan hotel history files for any missing entries
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="logs/compliance-history/"):
            for obj in page.get('Contents', []):
                s3_key = obj['Key']
                # Skip if not a hotel history file
                if not s3_key.endswith('.json') or s3_key.endswith('approval_log.json') or s3_key.endswith('audit_trail.json'):
                    continue
                
                try:
                    hotel_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                    hotel_history = json.loads(hotel_obj["Body"].read().decode("utf-8"))
                    hotel_id = s3_key.split('/')[-1].replace('.json', '')
                    
                    for task_id, task_entries in hotel_history.items():
                        if isinstance(task_entries, list):
                            for entry in task_entries:
                                uploaded_at = entry.get('uploaded_at')
                                if not uploaded_at:
                                    continue
                                    
                                entry_key = f"{hotel_id}_{task_id}_{uploaded_at}"
                                
                                # Determine status
                                if entry.get('approved'):
                                    status = 'approved'
                                elif entry.get('rejected') or entry.get('rejection_reason'):
                                    status = 'rejected'
                                else:
                                    status = 'pending'
                                
                                # Create standardized entry
                                standardized_entry = {
                                    "hotel_id": hotel_id,
                                    "task_id": task_id,
                                    "fileUrl": entry.get('fileUrl'),
                                    "filename": entry.get('filename'),
                                    "reportDate": entry.get('reportDate') or entry.get('report_date'),
                                    "uploadedAt": uploaded_at,
                                    "uploaded_by": entry.get('uploaded_by'),
                                    "status": status,
                                    "approved": entry.get('approved', False),
                                    "type": "upload",
                                    "reviewed_at": entry.get('reviewed_at'),
                                    "reviewed_by": entry.get('reviewed_by'),
                                    "rejection_reason": entry.get('rejection_reason')
                                }
                                
                                # Add or update with priority logic
                                if entry_key not in entries_map:
                                    entries_map[entry_key] = standardized_entry
                                else:
                                    current = entries_map[entry_key]
                                    current_status = current.get('status', 'pending')
                                    
                                    # Priority: approved/rejected > pending
                                    if current_status == 'pending' and status in ['approved', 'rejected']:
                                        entries_map[entry_key] = standardized_entry
                                    elif current_status == status and entry.get('reviewed_at'):
                                        entries_map[entry_key] = standardized_entry
                
                except Exception as e:
                    print(f"Error processing hotel file {s3_key}: {e}")
                    continue
    
    except Exception as e:
        print(f"Error scanning hotel files: {e}")
    
    # Convert back to list and save
    deduplicated_trail = list(entries_map.values())
    save_audit_trail(deduplicated_trail)
    
    return len(deduplicated_trail)


# IMPORTANT: DEBUG ENDPOINT MUST BE FIRST (before any /history/{hotel_id} pattern)
@router.get("/history/debug")
async def debug_audit_system():
    try:
        # Test creating audit trail
        try:
            test_trail = [{"test": "entry", "created_at": datetime.now().isoformat()}]
            save_audit_trail(test_trail)
            create_test = "SUCCESS - audit_trail.json created"
        except Exception as e:
            create_test = f"FAILED: {str(e)}"
        
        return {
            "create_test": create_test,
            "bucket": BUCKET_NAME,
            "audit_key": AUDIT_TRAIL_KEY
        }
    except Exception as e:
        return {"error": str(e)}


@router.post("/history/cleanup-duplicates")
async def cleanup_duplicate_entries():
    """Manual cleanup to remove duplicate entries from audit trail"""
    try:
        # This will deduplicate everything
        count = sync_pending_uploads_to_audit_trail()
        
        return {
            "success": True,
            "message": f"Cleanup completed - {count} unique entries remain",
            "total_entries": count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")


# SPECIFIC ENDPOINTS (before generic /history/{hotel_id})
@router.get("/history/pending")
async def get_pending_approvals():
    """Get only pending audit entries - FOR ADMIN DASHBOARD"""
    # First sync any new pending uploads
    sync_pending_uploads_to_audit_trail()
    
    # Return only TRULY pending entries (not approved or rejected)
    trail = load_audit_trail()
    pending_entries = [
        entry for entry in trail 
        if entry.get("status") == "pending" and not entry.get("approved") and not entry.get("rejected")
    ]
    
    return {"entries": pending_entries}


@router.get("/history/approval-log")
async def get_approval_log():
    """Get all audit entries (pending, approved, rejected) from the audit trail - FOR AUDIT PAGE"""
    # First sync any new pending uploads
    sync_pending_uploads_to_audit_trail()
    
    # Return the complete audit trail for the audit page
    trail = load_audit_trail()
    return {"entries": trail}


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


@router.post("/history/approve")
async def approve_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")
    performed_by = data.get("performed_by", "admin")

    # STEP 1: Update the individual hotel history file
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
    
    # STEP 2: Update the audit trail
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
        
        # If not found in trail, add it
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
    
    # STEP 3: Remove from pending approval log
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

    # Update hotel history - mark as rejected instead of deleting
    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    found = False
    
    for entry in entries:
        if entry.get("uploaded_at") == timestamp:
            entry["approved"] = False
            entry["rejected"] = True
            entry["rejection_reason"] = reason
            entry["reviewed_at"] = datetime.now().isoformat()
            entry["reviewed_by"] = performed_by
            found = True
            break
    
    if not found:
        raise HTTPException(status_code=404, detail="Entry not found")
    
    save_compliance_history(hotel_id, history)
    
    # Update audit trail
    try:
        trail = load_audit_trail()
        for trail_entry in trail:
            if (trail_entry.get("hotel_id") == hotel_id and 
                trail_entry.get("task_id") == task_id and 
                trail_entry.get("uploadedAt") == timestamp):
                
                trail_entry["status"] = "rejected"
                trail_entry["reviewed_at"] = datetime.now().isoformat()
                trail_entry["reviewed_by"] = performed_by
                trail_entry["approved"] = False
                trail_entry["rejection_reason"] = reason
                break
        
        save_audit_trail(trail)
    except Exception as e:
        print(f"Error updating audit trail: {e}")
    
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

    return {"success": True, "message": f"Rejected: {reason}"}


@router.delete("/history/delete")
async def delete_upload(request: Request):
    data = await request.json()
    hotel_id = data.get("hotel_id")
    task_id = data.get("task_id")
    timestamp = data.get("timestamp")

    # Remove from hotel history
    history = load_compliance_history(hotel_id)
    entries = history.get(task_id, [])
    filtered = [e for e in entries if e.get("uploaded_at") != timestamp]

    if len(filtered) == len(entries):
        raise HTTPException(status_code=404, detail="Entry not found")

    history[task_id] = filtered
    save_compliance_history(hotel_id, history)
    
    # Remove from audit trail
    try:
        trail = load_audit_trail()
        trail = [e for e in trail if not (
            e.get("hotel_id") == hotel_id and
            e.get("task_id") == task_id and
            e.get("uploaded_at") == timestamp
        )]
        save_audit_trail(trail)
    except Exception:
        pass
    
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


# GENERIC ENDPOINT MUST BE LAST (matches /history/{anything})
@router.get("/history/{hotel_id}")
async def get_hotel_history(hotel_id: str):
    """Get compliance history for a specific hotel"""
    history = load_compliance_history(hotel_id)
    return {
        "hotel_id": hotel_id,
        "history": history
    }
