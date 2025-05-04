import pdfplumber
import re
import io
from typing import List, Dict, Any


def fuzzy_match(label: str, keys: List[str]) -> str:
    label = label.lower().strip()
    for key in keys:
        if key in label:
            return key
    return ""


def parse_arden(pdf_bytes: bytes) -> dict:
    data = {
        "charges": [],
        "conflicts": [],
        "confidence": {},
    }

    # Extract text from PDF
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text_lines = []
        for page in pdf.pages:
            words = page.extract_words()
            lines = {}
            for word in words:
                y = round(word["top"], 1)
                if y not in lines:
                    lines[y] = []
                lines[y].append(word)
            for y in sorted(lines):
                line_text = " ".join([w["text"] for w in sorted(lines[y], key=lambda w: w['x0'])])
                text_lines.append(line_text)

    text = "\n".join(text_lines)

    def extract(pattern, group=1, default=None, cast=str):
        match = re.search(pattern, text)
        if match:
            try:
                return cast(match.group(group).replace(",", "").strip())
            except:
                return default
        return default

    # Header info
    data["supplier"] = "Arden Energy"
    data["billingRef"] = extract(r"Billing Ref\s+([^\n]+)")
    data["customerRef"] = extract(r"Customer Ref\s+([A-Z0-9]+)")
    data["billingPeriod"] = {
        "startDate": extract(r"Billing Period\s+(\d{2}-[A-Za-z]{3}-\d{2})"),
        "endDate": extract(r"to\s+(\d{2}-[A-Za-z]{3}-\d{2})"),
    }

    data["meterDetails"] = {
        "mprn": extract(r"MPRN\s+(\d+)", cast=int),
        "mic": {
            "value": extract(r"MIC\s+(\d+)", cast=int),
            "unit": "kVa"
        },
        "maxDemand": {
            "value": extract(r"Max Demand - Period\s+(\d+)", cast=int),
            "unit": "kVa"
        },
        "maxDemandDate": extract(r"Date\s+(\d{2}-[A-Za-z]{3}-\d{2})")
    }

    # Charges
    for line in text_lines:
        if re.search(r"@\s*\u20ac", line):
            parts = re.split(r"\s{2,}", line.strip())
            amount_match = re.findall(r"\u20ac([\d.,]+)", line)
            quantity_match = re.findall(r"(\d+[\d,.]*)\s*(kWh|kVa|kW|days)?", line)
            if amount_match and len(amount_match) >= 2:
                data["charges"].append({
                    "description": parts[0],
                    "quantity": int(quantity_match[0][0].replace(",", "")) if quantity_match else None,
                    "unit": quantity_match[0][1] if quantity_match else None,
                    "rate": float(amount_match[0].replace(",", "")),
                    "total": float(amount_match[1].replace(",", ""))
                })

    # Tax and totals
    data["taxDetails"] = {
        "electricityTax": extract(
            r"Electricity Tax\s+\d+\s*kWh\s*@\s*\u20ac[\d,.]+\s*\u20ac([\d,.]+)",
            group=1,
            cast=float,
            default=0.0
        ),
        "vatAmount": extract(r"VAT @ 9%\s+\u20ac([\d,.]+)", group=1, cast=float, default=0.0)
    }

    data["totalAmount"] = {
        "value": extract(r"Total \(This period\)\s+\u20ac([\d,.]+)", group=1, cast=float, default=0.0),
        "currency": "EUR"
    }

    # Sanity check: validate day + night total
    day_kwh = next((c for c in data["charges"] if "day" in c["description"].lower()), {})
    night_kwh = next((c for c in data["charges"] if "night" in c["description"].lower()), {})
    try:
        day_val = int(day_kwh.get("quantity", 0))
        night_val = int(night_kwh.get("quantity", 0))
        actual = day_val + night_val
        reported_total = extract(r"Total Units\s+(\d+)", cast=int, default=actual)
        if actual != reported_total:
            data["conflicts"].append(f"Mismatch in kWh total: {actual} != {reported_total}")
    except:
        data["conflicts"].append("Could not validate Day + Night kWh")

    return data
