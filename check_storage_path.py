from pathlib import Path

base = Path("storage") / "sera" / "energy" / "2023"

if base.exists():
    print(f"✅ Path exists: {base.resolve()}")
    elec = base / "electricity.json"
    gas = base / "gas.json"
    print("🔌 Electricity JSON found:", elec.exists())
    print("🔥 Gas JSON found:", gas.exists())
else:
    print("❌ Path does NOT exist:", base)
