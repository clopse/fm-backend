from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from app.parsers.arden import parse_arden

router = APIRouter()

def get_safe(d, default=""):
    return str(d) if d is not None else default

@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        raw = await parse_arden(await file.read())

        # Map charges by description for easier lookup
        charges_map = {}
        for item in raw.get("charges", []):
            key = item.get("description", "").lower()
            charges_map[key] = item

        # Get totals
        tax = raw.get("taxDetails", {})
        total = raw.get("totalAmount", {}).get("value", "")

        return {
            "billing_start": raw.get("billingPeriod", {}).get("startDate", ""),
            "billing_end": raw.get("billingPeriod", {}).get("endDate", ""),
            "day_kwh": get_safe(charges_map.get("day units", {}).get("quantity")),
            "night_kwh": get_safe(charges_map.get("night units", {}).get("quantity")),
            "mic": get_safe(raw.get("meterDetails", {}).get("mic")),
            "day_rate": get_safe(charges_map.get("day units", {}).get("rate")),
            "night_rate": get_safe(charges_map.get("night units", {}).get("rate")),
            "day_total": get_safe(charges_map.get("day units", {}).get("total")),
            "night_total": get_safe(charges_map.get("night units", {}).get("total")),
            "capacity_charge": get_safe(charges_map.get("capacity charge", {}).get("total")),
            "pso_levy": get_safe(charges_map.get("pso levy", {}).get("total")),
            "electricity_tax": get_safe(tax.get("electricityTax")),
            "vat": get_safe(tax.get("vatAmount")),
            "total_amount": get_safe(total),
            "full_data": raw  # Optional: include full raw parse for debugging/audits
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
