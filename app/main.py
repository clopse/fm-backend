# /app/main.py

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
    files,       # ✅ For service reports, renamed to files
)

app = FastAPI()

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

# Include Routers
app.include_router(utilities.router)
app.include_router(tenders.router)
app.include_router(drawings.router)
app.include_router(compliance.router, prefix="/compliance", tags=["compliance"])  # ✅ Corrected
app.include_router(files.router)

@app.get("/")
def read_root():
    return {"message": "JMK Project API is running 🚀"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
