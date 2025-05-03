import pdfplumber

def parse_arden(pdf_bytes: bytes) -> dict:
    data = {}
    with pdfplumber.open(pdf_bytes) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    
    if "Day Units" in text:
        try:
            data["Day Units"] = float(text.split("Day Units")[1].split("€")[0].strip().split()[-1])
        except:
            pass
    # Add more fields using similar logic
    return data
