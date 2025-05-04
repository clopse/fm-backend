from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from app.parsers.arden import parse_arden

router = APIRouter()

def safe_str(value):
    return str(value) if value is not None else ""

@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        content = await file.read()
        raw = parse_arden(content)  # ← FIXED: removed `await` from here

        charges = {c["description"].lower(): c for c in raw.get("charges", [])}
        mic_data = raw.get("meterDetails", {}).get("mic")
        mic = mic_data.get("value") if isinstance(mic_data, dict) else mic_data

        return {
            "billing_start": raw.get("billingPeriod", {}).get("startDate", ""),
            "billing_end": raw.get("billingPeriod", {}).get("endDate", ""),
            "day_kwh": safe_str(charges.get("day units", {}).get("quantity")),
            "night_kwh": safe_str(charges.get("night units", {}).get("quantity")),
            "mic": safe_str(mic),
            "day_rate": safe_str(charges.get("day units", {}).get("rate")),
            "night_rate": safe_str(charges.get("night units", {}).get("rate")),
            "day_total": safe_str(charges.get("day units", {}).get("total")),
            "night_total": safe_str(charges.get("night units", {}).get("total")),
            "capacity_charge": safe_str(charges.get("capacity charge", {}).get("total")),
            "pso_levy": safe_str(charges.get("pso levy", {}).get("total")),
            "electricity_tax": safe_str(raw.get("taxDetails", {}).get("electricityTax")),
            "vat": safe_str(raw.get("taxDetails", {}).get("vatAmount")),
            "total_amount": safe_str(raw.get("totalAmount", {}).get("value")),
            "full_data": raw
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Parse failed: {str(e)}"})
