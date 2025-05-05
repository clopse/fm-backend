from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse

router = APIRouter()

def get_safe(val, default=""):
    return str(val) if val is not None else default

@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        raw = parse_arden(await file.read())

        charges = {c["description"].lower(): c for c in raw.get("charges", [])}
        tax = raw.get("taxDetails", {})
        total = raw.get("totalAmount", {}).get("value", "")

        mic_raw = raw.get("meterDetails", {}).get("mic")
        mic = mic_raw.get("value") if isinstance(mic_raw, dict) else mic_raw

        return {
            "billing_start": raw.get("billingPeriod", {}).get("startDate", ""),
            "billing_end": raw.get("billingPeriod", {}).get("endDate", ""),
            "day_kwh": get_safe(charges.get("day units", {}).get("quantity")),
            "night_kwh": get_safe(charges.get("night units", {}).get("quantity")),
            "mic": get_safe(mic),
            "day_rate": get_safe(charges.get("day units", {}).get("rate")),
            "night_rate": get_safe(charges.get("night units", {}).get("rate")),
            "day_total": get_safe(charges.get("day units", {}).get("total")),
            "night_total": get_safe(charges.get("night units", {}).get("total")),
            "capacity_charge": get_safe(charges.get("capacity charge", {}).get("total")),
            "pso_levy": get_safe(charges.get("pso levy", {}).get("total")),
            "electricity_tax": get_safe(tax.get("electricityTax")),
            "vat": get_safe(tax.get("vatAmount")),
            "total_amount": get_safe(total),
            "full_data": raw
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
    return {"message": "Saved successfully"}
