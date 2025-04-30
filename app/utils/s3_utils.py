# FILE: app/utils/s3_utils.py

def generate_filename_from_dates(utility_type: str, billing_start: str, billing_end: str) -> str:
    """Generate a clean filename like 'gas01-31' from billing period dates."""
    try:
        start_day = billing_start.split("-")[0].zfill(2)
        end_day = billing_end.split("-")[0].zfill(2)
        return f"{utility_type[:4].lower()}{start_day}-{end_day}"
    except Exception:
        return f"{utility_type[:4].lower()}-unknown"
