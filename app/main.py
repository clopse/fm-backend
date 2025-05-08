from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

# Load environment variables from .env.local
load_dotenv()

# Routers
from app.routers import (
    uploads,
    utilities,
    drawings,
    tenders,
    compliance,
    files,                   # ✅ Service reports
    monthly_checklist,       # ✅ Checklist confirmation
    due_tasks,               # ✅ Tasks due logic
    compliance_score,        # ✅ Score calculation
    compliance_leaderboard,  # ✅ Leaderboard
    confirmations,           # ✅ Confirmation-only tasks
    compliance_history       # ✅ NEW: Full compliance history per hotel
)

app = FastAPI()

# Allow frontend connections
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

# Include all routers
app.include_router(utilities.router)
app.include_router(tenders.router)
app.include_router(drawings.router)
app.include_router(compliance.router, prefix="/compliance", tags=["compliance"])
app.include_router(files.router)
app.include_router(monthly_checklist.router, prefix="/api/compliance", tags=["monthly-checklist"])
app.include_router(due_tasks.router, prefix="/api/compliance", tags=["due-tasks"])
app.include_router(compliance_score.router, prefix="/api/compliance", tags=["compliance-score"])
app.include_router(compliance_leaderboard.router, prefix="/api/compliance", tags=["leaderboard"])
app.include_router(confirmations.router, prefix="/api/compliance", tags=["confirmations"])
app.include_router(compliance_history.router, prefix="/api/compliance", tags=["compliance-history"])  # ✅ NEW

# Base routes
@app.get("/")
def read_root():
    return {"message": "JMK Project API is running 🚀"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
