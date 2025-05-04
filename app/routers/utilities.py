from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse
from app.parsers.arden import parse_arden
from datetime import datetime

router = APIRouter()

def to_iso(datestr: str) -> str:
    """Convert '01-Jan-25' to '2025-01-01'"""
    try:
        return datetime.strptime(datestr, "%d-%b-%y").strftime("%Y-%m-%d")
    except:
        return ""

@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        contents = await file.read()         # ✅ Read raw bytes
        raw = parse_arden(contents)          # ✅ Pass bytes to parser

        charges = {c["description"]: c["amount"] for c in raw.get("charges", [])}
        consumption = {c["type"].lower(): c["units"]["value"] for c in raw.get("consumption", [])}

        def get(d, default=""):
            return str(d) if d is not None else default

        return {
            "billing_start": to_iso(raw.get("billingPeriod", {}).get("startDate", "")),
            "billing_end": to_iso(raw.get("billingPeriod", {}).get("endDate", "")),
            "day_kwh": get(consumption.get("day")),
            "night_kwh": get(consumption.get("night")),
            "mic": get(raw.get("meterDetails", {}).get("mic", {}).get("value")),
            "day_rate": "",
            "night_rate": "",
            "day_total": get(charges.get("Day Units")),
            "night_total": get(charges.get("Night Units")),
            "capacity_charge": get(charges.get("Capacity Charge")),
            "pso_levy": get(charges.get("PSO Levy")),
            "electricity_tax": get(raw.get("taxDetails", {}).get("electricityTax", {}).get("amount")),
            "vat": get(raw.get("taxDetails", {}).get("vatAmount")),
            "total_amount": get(raw.get("totalAmount", {}).get("value")),
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Parse failed: {str(e)}"})

@router.post("/utilities/parse-and-save")
async def parse_and_save(
    file: UploadFile = File(...),
    hotel_id: str = Form(...),
    utility_type: str = Form(...),
    supplier: str = Form(...),
    billing_start: str = Form(...),
    billing_end: str = Form(...),
    day_kwh: str = Form(""),
    night_kwh: str = Form(""),
    mic: str = Form(""),
    day_rate: str = Form(""),
    night_rate: str = Form(""),
    day_total: str = Form(""),
    night_total: str = Form(""),
    capacity_charge: str = Form(""),
    pso_levy: str = Form(""),
    electricity_tax: str = Form(""),
    vat: str = Form(""),
    total_amount: str = Form(""),
):
    # You can save to a DB, S3, or Excel here
    return {"message": "Saved successfully"}
