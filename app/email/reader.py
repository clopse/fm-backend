import sys
import os
import json
import re
from datetime import datetime
import fitz  # PyMuPDF

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from app.utils.excel_writer import update_energy_excel

SUPPLIER_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/suppliers.json"))
DEFAULT_HOTEL = "hiex"

def load_suppliers():
    try:
        with open(SUPPLIER_DATA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Failed to load supplier data: {e}")
        return []

SUPPLIERS = load_suppliers()

def detect_supplier_from_text(text):
    for supplier in SUPPLIERS:
        for alias in supplier["aliases"]:
            if alias.lower() in text.lower():
                return supplier
    return None

def parse_arden_energy_bill(text):
    data = {}

    match = re.search(r"Billing Period\s+(\d{2}-\w{3}-\d{2})\s+to\s+(\d{2}-\w{3}-\d{2})", text)
    if match:
        data["billing_start"] = match.group(1)
        data["billing_end"] = match.group(2)

    match = re.search(r"Day Units\s+([\d,]+)\s*kWh", text)
    if match:
        data["day_kwh"] = int(match.group(1).replace(",", ""))

    match = re.search(r"Night Units\s+([\d,]+)\s*kWh", text)
    if match:
        data["night_kwh"] = int(match.group(1).replace(",", ""))

    match = re.search(r"Total \(This period\)\s+€([\d,.]+)", text)
    if match:
        data["total_eur"] = float(match.group(1).replace(",", ""))

    match = re.search(r"VAT @ 9%\s+€[\d,.]+ @ 9\.0% €([\d,.]+)", text)
    if match:
        data["subtotal_eur"] = float(match.group(1).replace(",", ""))

    # Estimate subtotal if missing but total is available
    if not data.get("subtotal_eur") and data.get("total_eur"):
        data["subtotal_eur"] = round(data["total_eur"] / 1.09, 2)
        data["subtotal_eur_estimated"] = True
    else:
        data["subtotal_eur_estimated"] = False

    day_kwh = data.get("day_kwh", 0)
    night_kwh = data.get("night_kwh", 0)
    data["total_kwh"] = day_kwh + night_kwh

    if data.get("subtotal_eur") and data["total_kwh"] > 0:
        data["avg_rate"] = round(data["subtotal_eur"] / data["total_kwh"], 4)

    return data

def extract_text_from_pdf_from_bytes(pdf_bytes):
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return "".join(page.get_text() for page in doc)
    except Exception as e:
        print(f"⚠️ Error reading PDF: {e}")
        return ""

def calculate_confidence(data):
    score = 100
    if "billing_start" not in data: score -= 10
    if "billing_end" not in data: score -= 10
    if "day_kwh" not in data: score -= 10
    if "night_kwh" not in data: score -= 10
    if "total_kwh" not in data: score -= 10
    if "total_eur" not in data: score -= 20
    if "subtotal_eur" not in data: score -= 20
    return max(score, 0)

def parse_pdf(pdf_bytes: bytes, hotel: str = "hiex"):
    text = extract_text_from_pdf_from_bytes(pdf_bytes)
    supplier = detect_supplier_from_text(text)

    if not supplier:
        raise ValueError("Unknown supplier")

    if supplier["name"] == "Arden Energy":
        data = parse_arden_energy_bill(text)
        data["confidence_score"] = calculate_confidence(data)
        return data

    raise ValueError(f"No parser for supplier: {supplier['name']}")
