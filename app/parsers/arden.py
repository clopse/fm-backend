import pdfplumber
import re
import io
from datetime import datetime

def parse_arden(pdf_bytes: bytes) -> dict:
    data = {}

    # Extract text from all PDF pages
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text = "\n".join([
            page.extract_text() for page in pdf.pages if page.extract_text()
        ])

    def extract(pattern, group=1, default=None, cast=str):
        match = re.search(pattern, text)
        if match:
            try:
                return cast(match.group(group).replace(",", "").strip())
            except:
                return default
        return default

    def extract_date(raw):
        try:
            return datetime.strptime(raw, "%d-%b-%y").strftime("%Y-%m-%d")
        except:
            return ""

    # --- Basic Info ---
    data["supplier"] = "Arden Energy"
    data["billingRef"] = extract(r"Billing Ref\s+([^\n]+)")
    data["customerRef"] = extract(r"Customer Ref\s+([A-Z0-9]+)")

    # --- Billing Period ---
    start_raw = extract(r"Billing Period\s+(\d{2}-[A-Za-z]{3}-\d{2})")
    end_raw = extract(r"to\s+(\d{2}-[A-Za-z]{3}-\d{2})")
    data["billingPeriod"] = {
        "startDate": extract_date(start_raw),
        "endDate": extract_date(end_raw)
    }

    # --- Meter Details ---
    data["meterDetails"] = {
        "mprn": extract(r"MPRN\s+(\d+)", cast=int),
        "meterNumber": extract(r"Meter Number\s+(\S+)", default=""),
        "mic": extract(r"MIC\s+(\d+)", cast=int, default=0),
        "maxDemand": extract(r"Max Demand - Period\s+(\d+)", cast=int, default=0),
        "maxDemandDate": extract_date(extract(r"Date\s+(\d{2}-[A-Za-z]{3}-\d{2})"))
    }

    # --- Charges ---
    data["charges"] = []
    charge_pattern = re.compile(
        r"^(.*?)\s+(\d+(?:,\d{3})*|\d+)?\s*(kWh|kVa|kW|days|rate)?\s*@\s*€([\d.,]+)\s*€([\d.,]+)",
        re.MULTILINE
    )
    
    for match in charge_pattern.finditer(text):
        desc = match.group(1).strip()
        qty = match.group(2)
        unit = match.group(3) or ""
        rate = match.group(4)
        total = match.group(5)

        data["charges"].append({
            "description": desc,
            "quantity": int(qty.replace(",", "")) if qty else None,
            "unit": unit,
            "rate": float(rate.replace(",", "")),
            "total": float(total.replace(",", ""))
        })

    # --- Tax ---
    data["taxDetails"] = {
        "electricityTax": extract(r"Electricity Tax.*?€([\d.,]+)", group=1, cast=float, default=0.0),
        "vatAmount": extract(r"VAT @ \d+%\s+€([\d.,]+)", group=1, cast=float, default=0.0)
    }

    # --- Total ---
    data["totalAmount"] = {
        "value": extract(r"Total \(This period\)\s+€([\d.,]+)", group=1, cast=float, default=0.0),
        "currency": "EUR"
    }

    # --- Dummy customer (for now) ---
    data["customer"] = {
        "name": extract(r"Supply Address\s+(.+?)\s+\d{1,2}/", group=1, default=""),
        "address": {
            "street": "28/32 O'Connell St",
            "city": "Dublin",
            "postalCode": "Dublin 1"
        }
    }

    data["supplierContact"] = {
        "address": "Liffey Trust, Sheriff Street Upper, Dublin 1",
        "phone": ["01 517 5793", "1800 940 151"],
        "email": "info@ardenenergy.ie",
        "website": "www.ardenenergy.ie",
        "vatNumber": "9643703C"
    }

    return data
