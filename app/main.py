from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

load_dotenv()

# Routers
from app.routers import (
    uploads,
    utilities,
    water,              # <-- Add your new water.py router here!
    drawings,
    tenders,
    compliance,
    files,
    monthly_checklist,
    due_tasks,
    compliance_score,
    compliance_leaderboard,
    confirmations,
    compliance_tasks,
    audit,
    user,               # User management
    hotel_facilities    # Hotel facilities management
)

# Define nice tag display for your /docs UI
openapi_tags = [
    {"name": "utilities", "description": "Electricity, gas, and overall utilities endpoints."},
    {"name": "water", "description": "Water meter, Smartflow, and water bill endpoints."},
    {"name": "users", "description": "User management (create, list, edit, etc.)"},
    {"name": "hotels", "description": "Endpoints for hotel facilities and meta."},
    {"name": "compliance", "description": "Compliance checks, uploads, and scoring."},
    # Add other tag descriptions as needed
]

app = FastAPI(
    title="JMK Project API",
    version="1.0.0",
    openapi_tags=openapi_tags
)

# --- CORS ---
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

# --- Create Default Admin on Startup (if no users) ---
@app.on_event("startup")
async def create_admin_user():
    try:
        from app.routers.user import load_users, save_users, hash_password
        import uuid
        from datetime import datetime

        users = load_users()
        if not users:
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

# --- ROUTERS (all with tags/prefixes for nice docs) ---

app.include_router(utilities.router, prefix="/utilities", tags=["utilities"])
app.include_router(water.router, prefix="/water", tags=["water"])           # <--- NEW!
app.include_router(tenders.router)
app.include_router(drawings.router)
app.include_router(compliance.router, prefix="/compliance", tags=["compliance"])
app.include_router(files.router)
app.include_router(monthly_checklist.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(due_tasks.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(compliance_score.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(compliance_leaderboard.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(confirmations.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(compliance_tasks.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(audit.router, prefix="/api/compliance", tags=["compliance"])
app.include_router(user.router, prefix="/api/users", tags=["users"])
app.include_router(hotel_facilities.router, prefix="/api/hotels", tags=["hotels"])

# --- BASE ENDPOINTS ---
@app.get("/")
def read_root():
    return {"message": "JMK Project API is running 🚀"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

