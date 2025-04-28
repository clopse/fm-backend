import os
import json
from app.email.reader import parse_pdf
from datetime import datetime

BILLS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/manual_bills"))
RESULTS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/manual_results.json"))
PROCESSED_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/processed_manual_uploads.json"))

def load_processed():
    if os.path.exists(PROCESSED_PATH):
        with open(PROCESSED_PATH, "r", encoding="utf-8") as f:
            return set(json.load(f).get("files", []))
    return set()

def save_processed(processed):
    with open(PROCESSED_PATH, "w", encoding="utf-8") as f:
        json.dump({"files": list(processed)}, f, indent=2)

def load_results():
    if os.path.exists(RESULTS_PATH):
        with open(RESULTS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_results(results):
    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

def main():
    print(f"📁 Scanning {BILLS_DIR}...\n")

    processed_files = load_processed()
    results = load_results()

    new_results = []
    newly_processed = set()

    files = [f for f in os.listdir(BILLS_DIR) if f.endswith(".pdf")]
    if not files:
        print("⚠️ No PDF files found.")
        return

    for file in files:
        file_path = os.path.join(BILLS_DIR, file)

        if file in processed_files:
            print(f"⏩ Skipping already processed: {file}")
            continue

        with open(file_path, "rb") as f:
            pdf_bytes = f.read()

        try:
            data = parse_pdf(pdf_bytes, hotel="hiex")
            print(f"✅ Parsed: {file} — {data.get('billing_start', 'unknown')} to {data.get('billing_end', 'unknown')} (Confidence: {data.get('confidence_score', 0)}%)")

            new_results.append({
                "file": file,
                "parsed_at": datetime.now().isoformat(),
                "data": data
            })
            newly_processed.add(file)

        except Exception as e:
            print(f"❌ Error parsing {file}: {e}")

    if new_results:
        results.extend(new_results)
        save_results(results)
        save_processed(processed_files | newly_processed)
        print(f"\n📊 Done: {len(new_results)} new file(s) processed.")
    else:
        print("\n📭 No new files parsed.")

if __name__ == "__main__":
    main()