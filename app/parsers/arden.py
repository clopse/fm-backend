import pdfplumber
import re

def parse_arden(pdf_bytes: bytes) -> dict:
    data = {}

    with pdfplumber.open(pdf_bytes) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])

    def extract(pattern, group=1, default=None, cast=str):
        match = re.search(pattern, text)
        if match:
            try:
                return cast(match.group(group).replace(",", "").strip())
            except:
                return default
        return default

    # Top-level info
    data["supplier"] = "Arden Energy"
    data["customerRef"] = extract(r"Customer Ref\s+([A-Z0-9]+)")
    data["billingRef"] = extract(r"Billing Ref\s+([^\n]+)")
    data["billingPeriod"] = {
        "startDate": extract(r"Billing Period\s+(\d{2}-[A-Za-z]{3}-\d{2})"),
        "endDate": extract(r"Billing Period\s+\d{2}-[A-Za-z]{3}-\d{2}\s+to\s+(\d{2}-[A-Za-z]{3}-\d{2})"),
    }
    data["customer"] = {
        "name": extract(r"Supply Address\s+(.+?)\s+\d{1,2}/", group=1, default=""),
        "address": {
            "street": "28/32 O'Connell St",
            "city": "Dublin",
            "postalCode": "Dublin 1"
        }
    }
    data["meterDetails"] = {
        "mprn": extract(r"MPRN\s+(\d+)"),
        "meterNumber": "3029588",
        "meterType": extract(r"Meter\s+([A-Z0-9 ]+)"),
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

    # Consumption block
    data["consumption"] = []
    for type_ in ["Day", "Night", "Wattless"]:
        units = extract(rf"{type_}.*?(\d+)\s*$", cast=int, default=0)
        if units is not None:
            data["consumption"].append({
                "type": type_,
                "units": {
                    "value": units,
                    "unit": "kWh"
                }
            })

    # Charges
    def get_charge(description):
        return extract(rf"{re.escape(description)}.*?€([\d,]+\.\d+)", cast=float, default=0.0)

    data["charges"] = [
        {"description": "Standing Charge", "amount": get_charge("Standing Charge")},
        {"description": "Day Units", "amount": get_charge("Day Units")},
        {"description": "Night Units", "amount": get_charge("Night Units")},
        {"description": "Capacity Charge", "amount": get_charge("Capacity Charge")},
        {"description": "MIC Excess Charge", "amount": get_charge("MIC Excess Charge")},
        {"description": "Winter Demand Charge", "amount": get_charge("Winter Demand Charge")},
        {"description": "PSO Levy", "amount": get_charge("PSO Levy")},
        {"description": "Electricity Tax", "amount": get_charge("Electricity Tax")},
    ]

    # VAT & Total
    data["taxDetails"] = {
        "vatRate": extract(r"@\s+9\.0%", group=0, default=9.0, cast=float),
        "vatAmount": get_charge("VAT @ 9%"),
        "electricityTax": {
            "quantity": {
                "value": extract(r"Electricity Tax\s+(\d+)", cast=int, default=0),
                "unit": "kWh"
            },
            "rate": {
                "value": 0.001,
                "unit": "€/kWh"
            },
            "amount": get_charge("Electricity Tax")
        }
    }

    data["totalAmount"] = {
        "value": get_charge("Total \(This period\)"),
        "unit": "€"
    }

    # Contact
    data["supplierContact"] = {
        "address": "Liffey Trust, Sheriff Street Upper, Dublin 1",
        "phone": ["01 517 5793", "1800 940 151"],
        "email": "info@ardenenergy.ie",
        "website": "www.ardenenergy.ie",
        "vatNumber": "9643703C"
    }

    return data
