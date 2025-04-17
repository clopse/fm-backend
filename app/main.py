from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import uploads, utilities, tenders  # 👈 add this

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # adjust if deployed elsewhere
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Include your routers
app.include_router(uploads.router)
app.include_router(utilities.router)
app.include_router(tenders.router)  # 👈 add this

@app.get("/")
def read_root():
    return {"message": "JMK Project API is running 🚀"}

@app.get("/health")
def health_check():
    return {"status": "ok"}
