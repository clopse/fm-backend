from sqlalchemy import Column, String, Float, Integer, Date
from app.db import Base  # Adjust if your Base import path is different

class UtilityBill(Base):
    __tablename__ = "utility_bills"

    id = Column(Integer, primary_key=True, index=True)

    # ---------------------- GAS FIELDS ----------------------
    gas_supplierInfo_name = Column(String)
    gas_supplierInfo_vatRegNo = Column(String)
    gas_supplierInfo_phoneNumber = Column(String)
    gas_supplierInfo_email = Column(String)
    gas_supplierInfo_address_street = Column(String)
    gas_supplierInfo_address_city = Column(String)
    gas_supplierInfo_address_postalCode = Column(String)

    gas_customerInfo_name = Column(String)
    gas_customerInfo_address_street = Column(String)
    gas_customerInfo_address_city = Column(String)
    gas_customerInfo_address_postalCode = Column(String)
    gas_customerInfo_contactNumber = Column(String)

    gas_accountInfo_accountNumber = Column(String)
    gas_accountInfo_gprn = Column(String)
    gas_accountInfo_meterNumber = Column(String)
    gas_accountInfo_tariffCategory = Column(String)
    gas_accountInfo_paymentMethod = Column(String)

    gas_billSummary_invoiceNumber = Column(String)
    gas_billSummary_issueDate = Column(Date)
    gas_billSummary_dueDate = Column(Date)
    gas_billSummary_billingPeriodStartDate = Column(Date)
    gas_billSummary_billingPeriodEndDate = Column(Date)
    gas_billSummary_lastBillAmount = Column(Float)
    gas_billSummary_paymentReceivedAmount = Column(Float)
    gas_billSummary_balanceBroughtForward = Column(Float)
    gas_billSummary_netBillAmount = Column(Float)
    gas_billSummary_totalVatAmount = Column(Float)
    gas_billSummary_currentBillAmount = Column(Float)
    gas_billSummary_totalDueAmount = Column(Float)

    gas_meterReadings_previousReading = Column(String)
    gas_meterReadings_presentReading = Column(String)
    gas_meterReadings_unitsConsumed = Column(Integer)

    gas_consumptionDetails_consumptionValue = Column(Integer)
    gas_consumptionDetails_consumptionUnit = Column(String)
    gas_consumptionDetails_calibrationValue = Column(Float)
    gas_consumptionDetails_conversionFactor = Column(Float)
    gas_consumptionDetails_correctionFactor = Column(Float)

    # ------------------ ELECTRICITY FIELDS ------------------
    electricity_supplier = Column(String)
    electricity_customerRef = Column(String)
    electricity_billingRef = Column(String)

    electricity_customer_name = Column(String)
    electricity_customer_address_street = Column(String)
    electricity_customer_address_city = Column(String)
    electricity_customer_address_postalCode = Column(String)

    electricity_meterDetails_mprn = Column(String)
    electricity_meterDetails_meterNumber = Column(String)
    electricity_meterDetails_meterType = Column(String)
    electricity_meterDetails_mic_value = Column(Float)
    electricity_meterDetails_mic_unit = Column(String)
    electricity_meterDetails_maxDemand_value = Column(Float)
    electricity_meterDetails_maxDemand_unit = Column(String)
    electricity_meterDetails_maxDemandDate = Column(Date)

    electricity_consumption_day_kwh = Column(Integer)
    electricity_consumption_night_kwh = Column(Integer)
    electricity_consumption_wattless_kwh = Column(Integer)

    electricity_charge_StandingCharge = Column(Float)
    electricity_charge_DayUnits = Column(Float)
    electricity_charge_NightUnits = Column(Float)
    electricity_charge_LowPowerFactor = Column(Float)
    electricity_charge_CapacityCharge = Column(Float)
    electricity_charge_MICExcessCharge = Column(Float)
    electricity_charge_WinterDemandCharge = Column(Float)
    electricity_charge_PSOLevy = Column(Float)
    electricity_charge_ElectricityTax = Column(Float)

    electricity_taxDetails_vatRate = Column(Float)
    electricity_taxDetails_vatAmount = Column(Float)
    electricity_taxDetails_electricityTax_amount = Column(Float)

    electricity_totalAmount_value = Column(Float)
    electricity_supplierContact_address = Column(String)
    electricity_supplierContact_phone_1 = Column(String)
    electricity_supplierContact_phone_2 = Column(String)
    electricity_supplierContact_email = Column(String)
    electricity_supplierContact_website = Column(String)
    electricity_supplierContact_vatNumber = Column(String)
