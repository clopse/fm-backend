import os
import requests

FOLDER_PATH = "bills_to_test"
ENDPOINT = "http://localhost:8000/uploads/utilities"

results = []

for filename in os.listdir(FOLDER_PATH):
    if not filename.lower().endswith(".pdf"):
        continue

    file_path = os.path.join(FOLDER_PATH, filename)
    with open(file_path, "rb") as f:
        files = {"file": (filename, f, "application/pdf")}
        response = requests.post(ENDPOINT, files=files)

    print(f"📄 {filename}: {response.status_code}")
    try:
        data = response.json()
        results.append({
            "filename": filename,
            "status": data.get("status"),
            "confidence": data.get("data", {}).get("confidence_score"),
            "billing_start": data.get("data", {}).get("billing_start"),
            "billing_end": data.get("data", {}).get("billing_end"),
            "total_kwh": data.get("data", {}).get("total_kwh"),
            "total_eur": data.get("data", {}).get("total_eur"),
            "subtotal_eur": data.get("data", {}).get("subtotal_eur"),
        })
    except Exception as e:
        print(f"❌ Failed to parse response: {e}")
        results.append({
            "filename": filename,
            "status": "error",
            "error": str(e)
        })

# Optional: Print or save results
print("\n=== RESULTS ===")
for r in results:
    print(r)

# Or save to a file
with open("bulk_results.json", "w", encoding="utf-8") as out:
    import json
    json.dump(results, out, indent=2)
