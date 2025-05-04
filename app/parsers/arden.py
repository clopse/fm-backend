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
    data["meterDetails"]["mprn"] = extract(r"MPRN\s+(\d+)")
    data["meterDetails"]["meterNumber"] = extract(r"Meter Number\s+(\d+)")
    data["meterDetails"]["meterType"] = extract(r"Meter Type\s+([A-Za-z0-9 ]+)")
    data["meterDetails"]["mic"]["value"] = extract(r"MIC\s+(\d+)", cast=int)
    data["meterDetails"]["maxDemand"]["value"] = extract(r"Max Demand - Period\s+(\d+)", cast=int)
    data["meterDetails"]["maxDemandDate"] = extract(r"Date\s+(\d{2}-[A-Za-z]{3}-\d{2})")

    # Parse Charges
    for line in lines:
        if "@ €" in line:
            desc = line.split("@ €")[0].strip()
            amounts = re.findall(r"€([\d,.]+)", line)
            numbers = re.findall(r"(\d+[\d,.]*)\s*(kWh|kVa|kW|days)?", line)
            if amounts:
                quantity = numbers[0] if numbers else ("", "")
                charge = {
                    "description": line.strip(),
                    "quantity": int(quantity[0].replace(",", "")) if quantity[0] else None,
                    "unit": quantity[1] or "",
                    "rate": float(amounts[0].replace(",", "")) if len(amounts) > 0 else None,
                    "total": float(amounts[-1].replace(",", "")) if len(amounts) > 1 else float(amounts[0].replace(",", ""))
                }
                data["charges"].append(charge)

    # Consumption summary
    for label in ["Day Units", "Night Units", "Low Power Factor Units"]:
        c = next((x for x in data["charges"] if label.lower() in x["description"].lower()), None)
        if c:
            clean_label = label.replace(" Units", "").replace("Low Power Factor", "Wattless")
            data["consumption"].append({
                "type": clean_label,
                "units": {"value": c["quantity"], "unit": c["unit"]}
            })

    # Tax breakdown
    for line in lines:
        if "VAT @ 9%" in line:
            vat_matches = re.findall(r"€([\d,.]+)", line)
            if vat_matches:
                data["taxDetails"]["vatAmount"] = float(vat_matches[-1].replace(",", ""))
            break

    tax_line = next((x for x in data["charges"] if "Electricity Tax" in x["description"]), {})
    data["taxDetails"]["electricityTax"] = {
        "quantity": {"value": tax_line.get("quantity"), "unit": "kWh"},
        "rate": {"value": tax_line.get("rate"), "unit": "€/kWh"},
        "amount": tax_line.get("total")
    }

    # Total amount
    total = extract(r"Total \(This period\)\s+€([\d,.]+)", cast=float)
    data["totalAmount"] = {"value": total, "unit": "€"}

    return data
