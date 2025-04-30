import re
import fitz  # PyMuPDF

def extract_text_from_pdf_from_bytes(pdf_bytes):
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = "".join(page.get_text() for page in doc)
            print("📄 Extracted PDF text (first 1000 chars):")
            print(text[:1000])
            return text
    except Exception as e:
        print(f"⚠️ Error reading PDF: {e}")
        return ""

def calculate_confidence(data):
    score = 100
    for key in ["billing_start", "billing_end", "day_kwh", "night_kwh", "total_kwh", "total_eur"]:
        if key not in data:
            score -= 15
    return max(score, 0)

def parse_pdf(pdf_bytes: bytes, hotel: str = "unknown"):
    text = extract_text_from_pdf_from_bytes(pdf_bytes)

    if not text.strip():
        raise ValueError("PDF text could not be extracted.")

    data = {}

    # Billing Period
    match = re.search(r"Billing Period\s+(\d{2}-\w{3}-\d{2})\s+to\s+(\d{2}-\w{3}-\d{2})", text)
    if match:
        data["billing_start"] = match.group(1)
        data["billing_end"] = match.group(2)

    # Day Units
    match = re.search(r"Day Units\s+([\d,]+)\s*kWh.*?€([\d,.]+)", text)
    if match:
        data["day_kwh"] = int(match.group(1).replace(",", ""))
        data["day_total"] = float(match.group(2).replace(",", ""))

    # Night Units
    match = re.search(r"Night Units\s+([\d,]+)\s*kWh.*?€([\d,.]+)", text)
    if match:
        data["night_kwh"] = int(match.group(1).replace(",", ""))
        data["night_total"] = float(match.group(2).replace(",", ""))

    # Total (This period)
    match = re.search(r"Total \(This period\)\s+€([\d,.]+)", text)
    if match:
        data["total_eur"] = float(match.group(1).replace(",", ""))

    # VAT @ 9%
    match = re.search(r"VAT @ 9%\s+€[\d,.]+\s+@\s+9\.0%\s+€([\d,.]+)", text)
    if match:
        data["subtotal_eur"] = round(float(data["total_eur"]) - float(match.group(1).replace(",", "")), 2)

    # Fallback subtotal
    if not data.get("subtotal_eur") and data.get("total_eur"):
        data["subtotal_eur"] = round(data["total_eur"] / 1.09, 2)

    data["total_kwh"] = data.get("day_kwh", 0) + data.get("night_kwh", 0)
    data["confidence_score"] = calculate_confidence(data)

    return data
