from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from app.parsers.arden import parse_arden

router = APIRouter()

def safe_get(value):
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict) and "value" in value:
        return str(value["value"])
    return str(value) if value else ""

@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        content = await file.read()
        parsed = parse_arden(content)

        charges = parsed.get("charges", [])
        tax = parsed.get("taxDetails", {})
        meter = parsed.get("meterDetails", {})

        # Helper to find charges more reliably
        def find_charge(key):
            return next((c for c in charges if key in c.get("description", "").lower()), {})

        return {
            "billing_start": parsed.get("billingPeriod", {}).get("startDate", ""),
            "billing_end": parsed.get("billingPeriod", {}).get("endDate", ""),

            "day_kwh": safe_get(find_charge("day units").get("quantity")),
            "night_kwh": safe_get(find_charge("night units").get("quantity")),
            "mic": safe_get(meter.get("mic", {}).get("value")),
            "total_amount": safe_get(parsed.get("totalAmount", {}).get("value")),

            "day_rate": safe_get(find_charge("day units").get("rate")),
            "night_rate": safe_get(find_charge("night units").get("rate")),
            "day_total": safe_get(find_charge("day units").get("total")),
            "night_total": safe_get(find_charge("night units").get("total")),
            "capacity_charge": safe_get(find_charge("capacity charge").get("total")),
            "pso_levy": safe_get(find_charge("pso levy").get("total")),

            "electricity_tax": safe_get(tax.get("electricityTax", {}).get("amount")),
            "vat": safe_get(tax.get("vatAmount")),

            "full_data": parsed
        }

    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Parse failed: {str(e)}"})
