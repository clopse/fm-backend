# create_storage_folders.py

import os
from datetime import datetime

HOTELS = [
    "hiex", "moxy", "hida", "hbhdcc", "hbhe", "sera", "marina", "hiltonth", "belfast"
]

SECTIONS = [
    "tenders",
    "reports",
    "utilities",
    "safety-checks",
]

BASE_STORAGE_PATH = "storage"
CURRENT_YEAR = datetime.now().year
YEARS = [str(CURRENT_YEAR - i) for i in range(3)]  # Last 3 years including current

def create_storage_structure():
    for hotel in HOTELS:
        for year in YEARS:
            for section in SECTIONS:
                path = os.path.join(BASE_STORAGE_PATH, hotel, year, section)
                os.makedirs(path, exist_ok=True)
                print(f"Created: {path}")

if __name__ == "__main__":
    create_storage_structure()
