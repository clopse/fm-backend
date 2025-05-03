from pydantic import BaseModel
from typing import Optional, Literal, Dict

class UtilityBill(BaseModel):
    hotel_id: str
    supplier: str
    utility_type: Literal["electricity", "gas"]
    net_amount: Optional[float]
    vat_amount: Optional[float]
    total_amount: Optional[float]
    mic_kva: Optional[float]
    max_demand_kva: Optional[float]
    day_kwh: Optional[float]
    night_kwh: Optional[float]
    wattless_kwh: Optional[float]
    standing_charge: Optional[float]
    capacity_charge: Optional[float]
    mic_excess_charge: Optional[float]
    winter_demand_charge: Optional[float]
    pso_levy: Optional[float]
    electricity_tax: Optional[float]
    total_kwh: Optional[float]
    commodity_charge: Optional[float]
    carbon_tax: Optional[float]
    gas_capacity_charge: Optional[float]
    per_room_month_cost: Optional[float]
    total_kwh_per_room_month: Optional[float]
    raw_data: Optional[Dict]
