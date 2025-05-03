import pdfplumber
import re
from typing import Dict, Any
from datetime import datetime
from io import BytesIO


def extract_between(text: str, start: str, end: str = None) -> str:
    try:
        if end:
            return text.split(start)[1].split(end)[0].strip()
        return text.split(start)[1].strip()
    except (IndexError, ValueError):
        return ""


def parse_arden_text(text: str) -> Dict[str, Any]:
    data = {
        "supplier": "Arden Energy",
        "customerRef": extract_between(text, "Customer Reference:", "Billing Reference:").replace("Ref:", "").strip(),
        "billingRef": extract_between(text, "Billing Reference:", "Invoice Date:").strip(),
        "billingPeriod": {
            "startDate": "",
            "endDate": ""
        },
        "customer": {
            "name": "",
            "address": {
                "street": "",
                "city": "",
                "postalCode": ""
            }
        },
        "meterDetails": {
            "mprn": "",
            "meterNumber": "",
            "meterType": "",
            "mic": {"value": 0, "unit": "kVa"},
            "maxDemand": {"value": 0, "unit": "kVa"},
            "maxDemandDate": ""
        },
        "consumption": []
    }

    match = re.search(r"Bill Period:\s+(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})", text)
    if match:
        data["billingPeriod"]["startDate"] = datetime.strptime(match[1], "%d/%m/%Y").strftime("%Y-%m-%d")
        data["billingPeriod"]["endDate"] = datetime.strptime(match[2], "%d/%m/%Y").strftime("%Y-%m-%d")

    if "Bill To:" in text:
        customer_section = extract_between(text, "Bill To:", "MPRN")
        lines = customer_section.splitlines()
        if lines:
            data["customer"]["name"] = lines[0].strip()
            if len(lines) > 1:
                data["customer"]["address"]["street"] = lines[1].strip()
            if len(lines) > 2:
                data["customer"]["address"]["city"] = lines[2].strip()
            if len(lines) > 3:
                data["customer"]["address"]["postalCode"] = lines[3].strip()

    data["meterDetails"]["mprn"] = extract_between(text, "MPRN:", "Meter No:").strip()
    data["meterDetails"]["meterNumber"] = extract_between(text, "Meter No:", "Meter Type:").strip()
    data["meterDetails"]["meterType"] = extract_between(text, "Meter Type:", "MIC:").strip()

    mic_match = re.search(r"MIC:\s+(\d+)", text)
    if mic_match:
        data["meterDetails"]["mic"]["value"] = int(mic_match[1])

    demand_match = re.search(r"Max Demand:\s+(\d+)\s*(kVa)?\s*on\s*(\d{2}/\d{2}/\d{4})", text)
    if demand_match:
        data["meterDetails"]["maxDemand"]["value"] = int(demand_match[1])
        data["meterDetails"]["maxDemandDate"] = datetime.strptime(demand_match[3], "%d/%m/%Y").strftime("%Y-%m-%d")

    for line in text.splitlines():
        if line.startswith("Day Units"):
            try:
                value = int(re.findall(r"\d+", line)[-1])
                data["consumption"].append({"type": "Day", "units": {"value": value, "unit": "kWh"}})
            except: pass
        if line.startswith("Night Units"):
            try:
                value = int(re.findall(r"\d+", line)[-1])
                data["consumption"].append({"type": "Night", "units": {"value": value, "unit": "kWh"}})
            except: pass
        if line.startswith("Wattless Units"):
            try:
                value = int(re.findall(r"\d+", line)[-1])
                data["consumption"].append({"type": "Wattless", "units": {"value": value, "unit": "kWh"}})
            except: pass

    return data


def parse_arden(pdf_bytes: bytes) -> dict:
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    return parse_arden_text(text)
