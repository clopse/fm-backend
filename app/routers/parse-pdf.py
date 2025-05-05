@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        content = await file.read()
        parsed = parse_arden(content)

        # Use structured consumption data
        def get_consumption(type_name):
            c = next((c for c in parsed.get("consumption", []) if c["type"].lower() == type_name), {})
            return c.get("units", {}).get("value", "")

        # Use fuzzy charge match
        def find_charge(keyword):
            return next((c for c in parsed.get("charges", []) if keyword in c.get("description", "").lower()), {})

        tax = parsed.get("taxDetails", {})
        meter = parsed.get("meterDetails", {})

        return {
            "billing_start": parsed.get("billingPeriod", {}).get("startDate", ""),
            "billing_end": parsed.get("billingPeriod", {}).get("endDate", ""),

            "day_kwh": get_consumption("day"),
            "night_kwh": get_consumption("night"),
            "mic": meter.get("mic", {}).get("value", ""),
            "total_amount": parsed.get("totalAmount", {}).get("value", ""),

            "day_rate": find_charge("day units").get("rate", ""),
            "night_rate": find_charge("night units").get("rate", ""),
            "day_total": find_charge("day units").get("total", ""),
            "night_total": find_charge("night units").get("total", ""),
            "capacity_charge": find_charge("capacity charge").get("total", ""),
            "pso_levy": find_charge("pso levy").get("total", ""),
            "electricity_tax": tax.get("electricityTax", {}).get("amount", ""),
            "vat": tax.get("vatAmount", ""),

            "full_data": parsed
        }

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Parse failed: {str(e)}"}
        )
