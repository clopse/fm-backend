from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from app.parsers.arden import parse_arden

router = APIRouter()

def get(value, default=""):
    return str(value) if value is not None else default

@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        content = await file.read()
        raw = parse_arden(content)  # ✅ FIXED: no 'await' on parse_arden

        charges_map = {
            c.get("description", "").lower(): c for c in raw.get("charges", [])
        }
        tax = raw.get("taxDetails", {})
        total = raw.get("totalAmount", {}).get("value", "")

        mic_value = raw.get("meterDetails", {}).get("mic")
        mic = mic_value.get("value") if isinstance(mic_value, dict) else mic_value

        return {
            "billing_start": raw.get("billingPeriod", {}).get("startDate", ""),
            "billing_end": raw.get("billingPeriod", {}).get("endDate", ""),
            "day_kwh": get(charges_map.get("day units", {}).get("quantity")),
            "night_kwh": get(charges_map.get("night units", {}).get("quantity")),
            "mic": get(mic),
            "day_rate": get(charges_map.get("day units", {}).get("rate")),
            "night_rate": get(charges_map.get("night units", {}).get("rate")),
            "day_total": get(charges_map.get("day units", {}).get("total")),
            "night_total": get(charges_map.get("night units", {}).get("total")),
            "capacity_charge": get(charges_map.get("capacity charge", {}).get("total")),
            "pso_levy": get(charges_map.get("pso levy", {}).get("total")),
            "electricity_tax": get(tax.get("electricityTax")),
            "vat": get(tax.get("vatAmount")),
            "total_amount": get(total),
            "full_data": raw
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Parse failed: {str(e)}"})
