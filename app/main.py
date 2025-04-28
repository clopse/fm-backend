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
    safety,
    files,  # ✅ Now correctly import files (renamed from service_reports)
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
# app.include_router(uploads.router)   # (Enable uploads if you need later)
app.include_router(utilities.router)
app.include_router(tenders.router)
app.include_router(drawings.router)
app.include_router(safety.router)
app.include_router(files.router)  # ✅ Correct router here

@app.get("/")
def read_root():
    return {"message": "JMK Project API is running 🚀"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
