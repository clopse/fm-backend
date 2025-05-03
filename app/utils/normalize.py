# app/utils/normalize.py

def normalize_fields(data: dict, utility_type: str) -> dict:
    result = {
        "total_kwh": data.get("Total kWh", 0),
        "total_eur": data.get("Total €", 0),
        "day_kwh": data.get("Day Units"),
        "night_kwh": data.get("Night Units"),
        "subtotal_eur": data.get("Subtotal €"),
        "confidence_score": data.get("Confidence", 90),  # Example fallback
    }

    # Optional: logic based on utility type
    if utility_type == "gas":
        result.pop("day_kwh", None)
        result.pop("night_kwh", None)

    return result
