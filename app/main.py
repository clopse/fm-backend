from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

load_dotenv()

# Routers
from app.routers import (
    uploads,
    utilities,
    drawings,
    tenders,
    compliance,
    files,
    monthly_checklist,
    due_tasks,
    compliance_score,
    compliance_leaderboard,
    confirmations,
    compliance_history,
    compliance_tasks,
    audit,
    user,  # Add the user router
    hotel_facilities  # Add the hotel facilities router
)

app = FastAPI(title="JMK Project API", version="1.0.0")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://jmkfacilities.ie",
        "https://www.jmkfacilities.ie",
        "http://localhost:3000",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Auto-create admin user on startup
@app.on_event("startup")
async def create_admin_user():
    """Create default admin user if no users exist"""
    try:
        from app.routers.user import load_users, save_users, hash_password
        import uuid
        from datetime import datetime
        
        users = load_users()
        if not users:  # Only create if no users exist
            admin_id = str(uuid.uuid4())
            admin_user = {
                "name": "System Admin",
                "email": "admin@jmkhotels.ie",
                "role": "System Admin",
                "hotel": "All Hotels",
                "password": hash_password("admin123"),
                "status": "Active",
                "created_at": datetime.now().isoformat(),
                "last_login": None
            }
            users[admin_id] = admin_user
            save_users(users)
            print("✅ Admin user created successfully!")
            print("📧 Email: admin@jmkhotels.ie")
            print("🔑 Password: admin123")
            print("⚠️  Please change the password after first login!")
        else:
            print(f"ℹ️  Found {len(users)} existing users - skipping admin creation")
    except Exception as e:
        print(f"❌ Error creating admin user: {e}")

# Include routers
app.include_router(utilities.router)  # This gives you /utilities/... endpoints
app.include_router(tenders.router)
app.include_router(drawings.router)
app.include_router(compliance.router, prefix="/compliance", tags=["compliance"])
app.include_router(files.router)
app.include_router(monthly_checklist.router, prefix="/api/compliance", tags=["monthly-checklist"])
app.include_router(due_tasks.router, prefix="/api/compliance", tags=["due-tasks"])
app.include_router(compliance_score.router, prefix="/api/compliance", tags=["compliance-score"])
app.include_router(compliance_leaderboard.router, prefix="/api/compliance", tags=["leaderboard"])
app.include_router(confirmations.router, prefix="/api/compliance", tags=["confirmations"])
# app.include_router(compliance_history.router, prefix="/api/compliance", tags=["compliance-history"])  # DISABLED - replaced by audit.py
app.include_router(compliance_tasks.router, prefix="/api/compliance", tags=["compliance-tasks"])
app.include_router(audit.router, prefix="/api/compliance", tags=["audit"])

# User management router
app.include_router(user.router, prefix="/api/users", tags=["users"])

# Hotel facilities router
app.include_router(hotel_facilities.router, prefix="/api/hotels", tags=["hotels"])

# Base routes
@app.get("/")
def read_root():
    return {"message": "JMK Project API is running 🚀"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
