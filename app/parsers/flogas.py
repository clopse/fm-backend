import pdfplumber

def parse_flogas(pdf_bytes: bytes) -> dict:
    data = {}
    with pdfplumber.open(pdf_bytes) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    
    if "Gas Commodity Tariff" in text:
        try:
            data["Gas Commodity Tariff"] = float(text.split("Gas Commodity Tariff")[1].split("euro")[0].strip().split()[-1])
        except:
            pass
    # Add more fields using similar logic
    return data
