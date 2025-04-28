from fastapi import APIRouter, HTTPException
from pathlib import Path
import json

router = APIRouter()

# ✅ Correct relative path (you're running inside /backend)
BASE_DIR = Path("storage")

@router.get("/api/utilities/{hotel_id}/{year}")
async def get_utilities_data(hotel_id: str, year: int):
    hotel_path = BASE_DIR / hotel_id / "energy" / str(year)

    electricity_file = hotel_path / "electricity.json"
    gas_file = hotel_path / "gas.json"

    try:
        electricity_data = []
        gas_data = []

        if electricity_file.exists():
            with open(electricity_file, "r") as f:
                electricity_data = json.load(f)

        if gas_file.exists():
            with open(gas_file, "r") as f:
                gas_data = json.load(f)

        return {
            "electricity": electricity_data,
            "gas": gas_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading utility data: {str(e)}")
