from sqlalchemy.orm import Session
from sqlalchemy import text
from app.models.utilities import UtilityBill
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

def save_parsed_data_to_db(
    db: Session,
    hotel_id: str,
    utility_type: str,
    parsed: dict,
    s3_path: str
):
    try:
        bill = UtilityBill()
        bill.hotel_id = hotel_id
        bill.s3_json_path = s3_path

        if utility_type == "gas":
            si = parsed.get("supplierInfo", {})
            bill.gas_supplierInfo_name = si.get("name")
            bill.gas_supplierInfo_vatRegNo = si.get("vatRegNo")
            bill.gas_supplierInfo_phoneNumber = si.get("phoneNumber")
            bill.gas_supplierInfo_email = si.get("email")
            addr = si.get("address", {})
            bill.gas_supplierInfo_address_street = addr.get("street")
            bill.gas_supplierInfo_address_city = addr.get("city")
            bill.gas_supplierInfo_address_postalCode = addr.get("postalCode")

            ci = parsed.get("customerInfo", {})
            bill.gas_customerInfo_name = ci.get("name")
            caddr = ci.get("address", {})
            bill.gas_customerInfo_address_street = caddr.get("street")
            bill.gas_customerInfo_address_city = caddr.get("city")
            bill.gas_customerInfo_address_postalCode = caddr.get("postalCode")
            bill.gas_customerInfo_contactNumber = ci.get("contactNumber")

            ai = parsed.get("accountInfo", {})
            bill.gas_accountInfo_accountNumber = ai.get("accountNumber")
            bill.gas_accountInfo_gprn = ai.get("gprn")
            bill.gas_accountInfo_meterNumber = ai.get("meterNumber")
            bill.gas_accountInfo_tariffCategory = ai.get("tariffCategory")
            bill.gas_accountInfo_paymentMethod = ai.get("paymentMethod")

            bs = parsed.get("billSummary", {})
            bill.gas_billSummary_invoiceNumber = bs.get("invoiceNumber")
            bill.gas_billSummary_issueDate = safe_date(bs.get("issueDate"))
            bill.gas_billSummary_dueDate = safe_date(bs.get("dueDate"))
            bill.gas_billSummary_billingPeriodStartDate = safe_date(bs.get("billingPeriodStartDate"))
            bill.gas_billSummary_billingPeriodEndDate = safe_date(bs.get("billingPeriodEndDate"))
            bill.gas_billSummary_lastBillAmount = bs.get("lastBillAmount")
            bill.gas_billSummary_paymentReceivedAmount = bs.get("paymentReceivedAmount")
            bill.gas_billSummary_balanceBroughtForward = bs.get("balanceBroughtForward")
            bill.gas_billSummary_netBillAmount = bs.get("netBillAmount")
            bill.gas_billSummary_totalVatAmount = bs.get("totalVatAmount")
            bill.gas_billSummary_currentBillAmount = bs.get("currentBillAmount")
            bill.gas_billSummary_totalDueAmount = bs.get("totalDueAmount")

            mr = parsed.get("meterReadings", {})
            bill.gas_meterReadings_previousReading = mr.get("previousReading")
            bill.gas_meterReadings_presentReading = mr.get("presentReading")
            bill.gas_meterReadings_unitsConsumed = mr.get("unitsConsumed")

            cd = parsed.get("consumptionDetails", {})
            bill.gas_consumptionDetails_consumptionValue = cd.get("consumptionValue")
            bill.gas_consumptionDetails_consumptionUnit = cd.get("consumptionUnit")
            bill.gas_consumptionDetails_calibrationValue = cd.get("calibrationValue")
            bill.gas_consumptionDetails_conversionFactor = cd.get("conversionFactor")
            bill.gas_consumptionDetails_correctionFactor = cd.get("correctionFactor")

        elif utility_type == "electricity":
            bill.electricity_supplier = parsed.get("supplier")
            bill.electricity_customerRef = parsed.get("customerRef")
            bill.electricity_billingRef = parsed.get("billingRef")

            cust = parsed.get("customer", {})
            bill.electricity_customer_name = cust.get("name")
            ca = cust.get("address", {})
            bill.electricity_customer_address_street = ca.get("street")
            bill.electricity_customer_address_city = ca.get("city")
            bill.electricity_customer_address_postalCode = ca.get("postalCode")

            meter = parsed.get("meterDetails", {})
            bill.electricity_meterDetails_mprn = meter.get("mprn")
            bill.electricity_meterDetails_meterNumber = meter.get("meterNumber")
            bill.electricity_meterDetails_meterType = meter.get("meterType")
            bill.electricity_meterDetails_mic_value = meter.get("mic", {}).get("value")
            bill.electricity_meterDetails_mic_unit = meter.get("mic", {}).get("unit")
            bill.electricity_meterDetails_maxDemand_value = meter.get("maxDemand", {}).get("value")
            bill.electricity_meterDetails_maxDemand_unit = meter.get("maxDemand", {}).get("unit")
            bill.electricity_meterDetails_maxDemandDate = safe_date(meter.get("maxDemandDate"))

            consumption = parsed.get("consumption", [])
            for entry in consumption:
                t = entry.get("type", "").lower()
                val = entry.get("units", {}).get("value")
                if t == "day":
                    bill.electricity_consumption_day_kwh = val
                elif t == "night":
                    bill.electricity_consumption_night_kwh = val
                elif t == "wattless":
                    bill.electricity_consumption_wattless_kwh = val

            charges = parsed.get("charges", [])
            for ch in charges:
                desc = ch.get("description", "").lower()
                amt = ch.get("amount")
                if "standing" in desc:
                    bill.electricity_charge_StandingCharge = amt
                elif "day units" in desc:
                    bill.electricity_charge_DayUnits = amt
                elif "night units" in desc:
                    bill.electricity_charge_NightUnits = amt
                elif "low power" in desc:
                    bill.electricity_charge_LowPowerFactor = amt
                elif "capacity" in desc:
                    bill.electricity_charge_CapacityCharge = amt
                elif "mic excess" in desc:
                    bill.electricity_charge_MICExcessCharge = amt
                elif "winter demand" in desc:
                    bill.electricity_charge_WinterDemandCharge = amt
                elif "pso levy" in desc:
                    bill.electricity_charge_PSOLevy = amt
                elif "electricity tax" in desc:
                    bill.electricity_charge_ElectricityTax = amt

            tax = parsed.get("taxDetails", {})
            bill.electricity_taxDetails_vatRate = tax.get("vatRate")
            bill.electricity_taxDetails_vatAmount = tax.get("vatAmount")
            bill.electricity_taxDetails_electricityTax_amount = tax.get("electricityTax", {}).get("amount")

            bill.electricity_totalAmount_value = parsed.get("totalAmount", {}).get("value")

            supplier_contact = parsed.get("supplierContact", {})
            bill.electricity_supplierContact_address = supplier_contact.get("address")
            phones = supplier_contact.get("phone", [])
            if isinstance(phones, list):
                bill.electricity_supplierContact_phone_1 = phones[0] if len(phones) > 0 else None
                bill.electricity_supplierContact_phone_2 = phones[1] if len(phones) > 1 else None
            bill.electricity_supplierContact_email = supplier_contact.get("email")
            bill.electricity_supplierContact_website = supplier_contact.get("website")
            bill.electricity_supplierContact_vatNumber = supplier_contact.get("vatNumber")

        db.add(bill)
        db.commit()
        db.refresh(bill)
        return bill

    except SQLAlchemyError as e:
        db.rollback()
        raise RuntimeError(f"Database error while saving utility bill: {str(e)}")

def safe_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else None
    except:
        return None
