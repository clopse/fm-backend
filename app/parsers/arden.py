import pdfplumber
import re
import io

def parse_arden(pdf_bytes: bytes) -> dict:
    data = {}

    # Read text content from PDF
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])

    def extract(pattern, group=1, default=None, cast=str):
        match = re.search(pattern, text)
        if match:
            try:
                return cast(match.group(group).replace(",", "").strip())
            except:
                return default
        return default

    # Core metadata
    data["supplier"] = "Arden Energy"
    data["billingRef"] = extract(r"Billing Ref\s+([^\n]+)")
    data["customerRef"] = extract(r"Customer Ref\s+([A-Z0-9]+)")
    data["billingPeriod"] = {
        "startDate": extract(r"Billing Period\s+(\d{2}-[A-Za-z]{3}-\d{2})"),
        "endDate": extract(r"to\s+(\d{2}-[A-Za-z]{3}-\d{2})")
    }

    data["meterDetails"] = {
        "mprn": extract(r"MPRN\s+(\d+)"),
        "meterNumber": extract(r"Meter Number\s+(\S+)"),
        "mic": {
            "value": extract(r"MIC\s+(\d+)", cast=int, default=0),
            "unit": "kVa"
        },
        "maxDemand": {
            "value": extract(r"Max Demand - Period\s+(\d+)", cast=int, default=0),
            "unit": "kVa"
        },
        "maxDemandDate": extract(r"Date\s+(\d{2}-[A-Za-z]{3}-\d{2})")
    }

    # Extract charge lines with quantities, rates, totals
    data["charges"] = []
    charge_pattern = re.compile(
        r"^(.*?)\s+(\d+(?:,\d{3})*|\d+)?\s*(kWh|kVa|kW|days|rate)?\s*@\s*\u20ac([\d.,]+)\s*\u20ac([\d.,]+)",
        re.MULTILINE
    )

    for match in charge_pattern.finditer(text):
        description = match.group(1).strip()
        quantity = match.group(2)
        unit = match.group(3) or ""
        rate = match.group(4)
        total = match.group(5)

        data["charges"].append({
            "description": description,
            "quantity": int(quantity.replace(",", "")) if quantity else None,
            "unit": unit,
            "rate": float(rate.replace(",", "")),
            "total": float(total.replace(",", ""))
        })

    # Tax breakdown
    data["taxDetails"] = {
        "electricityTax": extract(
            r"Electricity Tax\s+(\d+(?:,\d{3})*)\s*kWh\s*@\s*\u20ac[\d,.]+\s*\u20ac([\d,.]+)",
            group=2,
            cast=float,
            default=0.0
        ),
        "vatAmount": extract(r"VAT @ 9%\s+\u20ac([\d,.]+)", group=1, cast=float, default=0.0)
    }

    data["totalAmount"] = {
        "value": extract(r"Total \\(This period\\)\s+\u20ac([\d,.]+)", group=1, cast=float, default=0.0),
        "currency": "EUR"
    }

    # Static customer address (can be dynamic later)
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
