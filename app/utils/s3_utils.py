from datetime import datetime

def month_short_name(date_str):
    """Helper to get short month name from DD-MMM-YY format"""
    try:
        parsed = datetime.strptime(date_str, "%d-%b-%y")
        return parsed.strftime("%b").lower(), parsed.strftime("%d")
    except ValueError:
        return "invalid", "xx"

def generate_filename_from_dates(utility_type: str, start: str, end: str) -> str:
    """
    Builds filename like:
    - gas_jan06-feb06
    - elec_feb07-mar08
    """
    start_month, start_day = month_short_name(start)
    end_month, end_day = month_short_name(end)

    if "electric" in utility_type.lower():
        prefix = "elec"
    elif "gas" in utility_type.lower():
        prefix = "gas"
    elif "water" in utility_type.lower():
        prefix = "water"
    else:
        prefix = "util"

    return f"{prefix}_{start_month}{start_day}-{end_month}{end_day}"
