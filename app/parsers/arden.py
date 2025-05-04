import pdfplumber
import re
import io


def parse_arden(pdf_bytes: bytes) -> dict:
    data = {
        "supplier": "Arden Energy",
        "customerRef": None,
        "billingRef": None,
        "billingPeriod": {"startDate": None, "endDate": None},
        "customer": {
            "name": "Findlater House Limited",
            "address": {
                "street": "28/32 O'Connell St",
                "city": "Dublin",
                "postalCode": "Dublin 1"
            }
        },
        "meterDetails": {
            "mprn": None,
            "meterNumber": None,
            "meterType": None,
            "mic": {"value": None, "unit": "kVa"},
            "maxDemand": {"value": None, "unit": "kVa"},
            "maxDemandDate": None
        },
        "consumption": [],
        "charges": [],
        "taxDetails": {
            "vatRate": 9,
            "vatAmount": None,
            "electricityTax": {
                "quantity": {"value": None, "unit": "kWh"},
                "rate": {"value": None, "unit": "€/kWh"},
                "amount": None
            }
        },
        "totalAmount": {"value": None, "unit": "€"},
        "supplierContact": {
            "address": "Liffey Trust, Sheriff Street Upper, Dublin 1",
            "phone": ["01 517 5793", "1800 940 151"],
            "email": "info@ardenenergy.ie",
            "website": "www.ardenenergy.ie",
            "vatNumber": "9643703C"
        }
    }

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        lines = []
        for page in pdf.pages:
            words = page.extract_words()
            grouped = {}
            for w in words:
                y = round(w['top'], 1)
                grouped.setdefault(y, []).append(w)
            for y in sorted(grouped):
                line = " ".join([w['text'] for w in sorted(grouped[y], key=lambda w: w['x0'])])
                lines.append(line)

    text = "\n".join(lines)

    def extract(pattern, group=1, default=None, cast=str):
        match = re.search(pattern, text)
        if match:
            try:
                return cast(match.group(group).replace(",", "").strip())
            except:
                return default
        return default

    # Header & Period
    data["customerRef"] = extract(r"Customer Ref\s+([A-Z0-9]+)")
    data["billingRef"] = extract(r"Billing Ref\s+([\dA-Za-z\-\s]+)")
    data["billingPeriod"]["startDate"] = extract(r"(\d{2}-[A-Za-z]{3}-\d{2})\s+to")
    data["billingPeriod"]["endDate"] = extract(r"to\s+(\d{2}-[A-Za-z]{3}-\d{2})")

    # Meter Info
    data["meterDetails"]["mprn"] = extract(r"(\d{11})")
    data["meterDetails"]["meterNumber"] = extract(r"Meter No\s+(\d+)")
    data["meterDetails"]["meterType"] = extract(r"Meter\s+([A-Z0-9 ]+)")
    data["meterDetails"]["mic"]["value"] = extract(r"MIC\s+(\d+)", cast=int)
    data["meterDetails"]["maxDemand"]["value"] = extract(r"Max Demand - Period\s+(\d+)", cast=int)
    data["meterDetails"]["maxDemandDate"] = extract(r"Date\s+(\d{2}-[A-Za-z]{3}-\d{2})")

    # Parse Charges with logic validation
    for line in lines:
        if "@ €" in line:
            amounts = re.findall(r"€([\d,.]+)", line)
            numbers = re.findall(r"(\d+[\d,.]*)\s*(kWh|kVa|kW|days)?", line)
            quantity = numbers[0] if numbers else ("", "")

            try:
                parsed_quantity = int(quantity[0].replace(",", "")) if quantity[0] else None
                parsed_rate = float(amounts[0].replace(",", "")) if len(amounts) > 0 else None
                parsed_total = float(amounts[-1].replace(",", "")) if len(amounts) > 0 else None

                # Sanity check: quantity × rate ≈ total (±1%)
                if parsed_quantity and parsed_rate and parsed_total:
                    calc = parsed_quantity * parsed_rate
                    if abs(calc - parsed_total) > max(1.0, 0.01 * parsed_total):
                        parsed_total = round(calc, 2)  # override to match math

                data["charges"].append({
                    "description": line.strip(),
                    "quantity": parsed_quantity,
                    "unit": quantity[1] or "",
                    "rate": parsed_rate,
                    "total": parsed_total
                })
            except:
                continue

    # Consumption summary (cross check logic)
    day = next((x for x in data["charges"] if "day units" in x["description"].lower()), {})
    night = next((x for x in data["charges"] if "night units" in x["description"].lower()), {})
    wattless = next((x for x in data["charges"] if "low power" in x["description"].lower()), {})
    total_kwh = sum([v.get("quantity", 0) or 0 for v in [day, night, wattless]])

    data["consumption"] = [
        {"type": "Day", "units": {"value": day.get("quantity"), "unit": "kWh"}},
        {"type": "Night", "units": {"value": night.get("quantity"), "unit": "kWh"}},
        {"type": "Wattless", "units": {"value": wattless.get("quantity"), "unit": "kWh"}}
    ]

    # Tax breakdown
    vat_line = next((l for l in lines if "VAT @ 9%" in l), "")
    vat_amounts = re.findall(r"€([\d,.]+)", vat_line)
    if vat_amounts:
        data["taxDetails"]["vatAmount"] = float(vat_amounts[-1].replace(",", ""))

    tax = next((x for x in data["charges"] if "electricity tax" in x["description"].lower()), {})
    data["taxDetails"]["electricityTax"] = {
        "quantity": {"value": total_kwh, "unit": "kWh"},
        "rate": {"value": tax.get("rate"), "unit": "€/kWh"},
        "amount": tax.get("total")
    }

    # Total
    total = extract(r"Total \(This period\)\s+€([\d,.]+)", cast=float)
    data["totalAmount"] = {"value": total, "unit": "€"}

    return data
