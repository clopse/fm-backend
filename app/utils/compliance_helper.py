# Without helper - you'd copy this in every file:
try:
    key = f"hotels/facilities/{hotel_id}tasks.json"
    response = s3.get_object(Bucket=BUCKET, Key=key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    # ... extract tasks logic
except s3.exceptions.NoSuchKey:
    return []

# With helper - just one line:
tasks = get_hotel_compliance_tasks(hotel_id)
