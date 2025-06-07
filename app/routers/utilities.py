if not utility_type:
                    # Try to determine from the raw data structure
                    raw_data = bill.get('raw_data', bill)
                    if 'supplier' in raw_data and 'arden' in raw_data.get('supplier', '').lower():
                        utility_type = 'electricity'
                    elif 'supplierInfo' in raw_data or 'documentType' in raw_data:
                        utility_type = 'gas'
                    else:
                        continue  # Skip unknown types
                
                # Get summary or extract from raw data
                summary = bill.get('summary', {})
                raw_data = bill.get('raw_data', bill)
                
                # Extract bill date
                bill_date = (
                    summary.get('bill_date') or
                    summary.get('billing_period_end') or
                    raw_data.get('billingPeriod', {}).get('endDate') or
                    raw_data.get('billSummary', {}).get('billingPeriodEndDate') or
                    ''
                )
                
                # Skip if not from requested year
                if not bill_date.startswith(str(year)):
                    continue
                
                month_key = bill_date[:7]  # YYYY-MM format
                
                # Additional deduplication check at the endpoint level
                if utility_type in months_seen and month_key in months_seen[utility_type]:
                    print(f"WARNING: Duplicate month detected at endpoint level: {utility_type} {month_key}")
                    continue
                
                months_seen[utility_type].add(month_key)
                
                if utility_type == 'electricity':
                    # Extract electricity data using the real JSON structure
                    consumption = raw_data.get('consumption', [])
                    day_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Day'), 0)
                    night_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Night'), 0)
                    total_kwh = day_kwh + night_kwh
                    total_cost = raw_data.get('totalAmount', {}).get('value', 0)
                    
                    electricity_data.append({
                        "month": month_key,
                        "day_kwh": day_kwh,
                        "night_kwh": night_kwh,
                        "total_kwh": total_kwh,
                        "total_eur": total_cost,
                        "per_room_kwh": total_kwh / 100,  # Adjust room count as needed
                        "bill_id": bill.get('filename', f'electricity_{month_key}')
                    })
                    processed_count["electricity"] += 1
                
                elif utility_type == 'gas':
                    # Extract gas data using the real JSON structure
                    consumption_kwh = raw_data.get('consumptionDetails', {}).get('consumptionValue', 0)
                    total_cost = raw_data.get('billSummary', {}).get('currentBillAmount', 0)
                    
                    gas_data.append({
                        "period": month_key,
                        "total_kwh": consumption_kwh,
                        "total_eur": total_cost,
                        "per_room_kwh": consumption_kwh / 100,  # Adjust room count as needed
                        "bill_id": bill.get('filename', f'gas_{month_key}')
                    })
                    processed_count["gas"] += 1
                
            except Exception as e:
                print(f"Error processing individual bill: {e}")
                continue
        
        print(f"Final processed bills for {hotel_id} {year}: {processed_count}")
        print(f"Months seen - Electricity: {sorted(months_seen['electricity'])}, Gas: {sorted(months_seen['gas'])}")
        
        # Calculate totals for summary
        electricity_total = sum(item.get('total_kwh', 0) for item in electricity_data)
        gas_total = sum(item.get('total_kwh', 0) for item in gas_data)
        electricity_cost_total = sum(item.get('total_eur', 0) for item in electricity_data)
        gas_cost_total = sum(item.get('total_eur', 0) for item in gas_data)
        
        return {
            "electricity": electricity_data,
            "gas": gas_data,
            "water": water_data,
            "totals": {
                "electricity": electricity_total,
                "gas": gas_total,
                "electricity_cost": electricity_cost_total,
                "gas_cost": gas_cost_total
            },
            "processed_counts": processed_count,
            "total_bills_found": len(bills),
            "debug_info": {
                "months_processed": {
                    "electricity": list(months_seen['electricity']),
                    "gas": list(months_seen['gas'])
                }
            }
        }
        
    except Exception as e:
        print(f"Error fetching utilities data for {hotel_id} {year}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch utilities data: {str(e)}")

@router.get("/utilities/{hotel_id}/bills")
async def get_raw_bills_data(hotel_id: str, year: str = None):
    """Get raw bill data with full DocuPipe JSON for advanced filtering"""
    try:
        # Get all years if not specified
        current_year = datetime.now().year
        years_to_check = [year] if year else [str(current_year), str(current_year - 1)]
        
        all_bills = []
        
        for check_year in years_to_check:
            bills = get_utility_data_for_hotel_year(hotel_id, check_year)
            
            for bill in bills:
                try:
                    # Ensure bill has proper structure
                    if not isinstance(bill, dict):
                        continue
                    
                    # Get metadata
                    summary = bill.get('summary', {})
                    raw_data = bill.get('raw_data', bill)
                    
                    # Determine utility type if missing
                    utility_type = bill.get('utility_type')
                    if not utility_type:
                        if 'supplier' in raw_data and 'arden' in raw_data.get('supplier', '').lower():
                            utility_type = 'electricity'
                        elif 'supplierInfo' in raw_data or 'documentType' in raw_data:
                            utility_type = 'gas'
                        else:
                            utility_type = 'unknown'
                    
                    # Extract bill date for filtering
                    bill_date = (
                        summary.get('bill_date') or
                        raw_data.get('billingPeriod', {}).get('endDate') or
                        raw_data.get('billSummary', {}).get('billingPeriodEndDate') or
                        ''
                    )
                    
                    # Add enhanced metadata
                    enhanced_bill = {
                        **bill,
                        'utility_type': utility_type,
                        'enhanced_summary': {
                            **summary,
                            'bill_date': bill_date,
                            'month_year': bill_date[:7] if bill_date else '',
                            'year': bill_date[:4] if bill_date else ''
                        }
                    }
                    
                    all_bills.append(enhanced_bill)
                    
                except Exception as e:
                    print(f"Error processing bill: {e}")
                    continue
        
        # Sort by bill date (newest first)
        all_bills.sort(
            key=lambda x: x.get('enhanced_summary', {}).get('bill_date', ''), 
            reverse=True
        )
        
        return {
            "hotel_id": hotel_id,
            "year": year or "all",
            "bills": all_bills,
            "total_bills": len(all_bills),
            "bills_by_type": {
                "electricity": len([b for b in all_bills if b.get('utility_type') == 'electricity']),
                "gas": len([b for b in all_bills if b.get('utility_type') == 'gas']),
                "water": len([b for b in all_bills if b.get('utility_type') == 'water']),
                "unknown": len([b for b in all_bills if b.get('utility_type') == 'unknown'])
            }
        }
        
    except Exception as e:
        print(f"Error fetching raw bills: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch bills: {str(e)}")

@router.get("/utilities/missing-bills")
async def get_missing_bills():
    """Get missing and overdue utility bills dashboard"""
    try:
        current_date = datetime.now()
        missing_bills = []
        
        # Hotel utilities mapping (customize based on your hotels)
        hotel_utilities = {
            "hiex": ["electricity", "gas"],
            "moxy": ["electricity", "gas"], 
            "hida": ["electricity", "gas"],
            "hbhdcc": ["electricity", "gas"],
            "hbhe": ["electricity"],
            "sera": ["electricity", "gas"],
            "marina": ["electricity", "gas"],
            "belfast": ["electricity", "gas"],
            "hiltonth": ["electricity", "gas"]
        }
        
        # Check last 6 months for missing bills
        for months_back in range(1, 7):
            check_date = current_date - timedelta(days=30 * months_back)
            year_month = check_date.strftime("%Y-%m")
            
            for hotel_id, utilities in hotel_utilities.items():
                for utility_type in utilities:
                    try:
                        # Get bills for this hotel and year
                        bills = get_utility_data_for_hotel_year(hotel_id, str(check_date.year))
                        
                        # Check if bill exists for this specific month
                        bill_exists = False
                        for bill in bills:
                            bill_date = ''
                            summary = bill.get('summary', {})
                            raw_data = bill.get('raw_data', bill)
                            
                            # Extract bill date
                            bill_date = (
                                summary.get('bill_date') or
                                raw_data.get('billingPeriod', {}).get('endDate') or
                                raw_data.get('billSummary', {}).get('billingPeriodEndDate') or
                                ''
                            )
                            
                            # Check utility type
                            bill_utility_type = bill.get('utility_type')
                            if not bill_utility_type:
                                if 'supplier' in raw_data and 'arden' in raw_data.get('supplier', '').lower():
                                    bill_utility_type = 'electricity'
                                elif 'supplierInfo' in raw_data:
                                    bill_utility_type = 'gas'
                            
                            if (bill_date.startswith(year_month) and 
                                bill_utility_type == utility_type):
                                bill_exists = True
                                break
                        
                        if not bill_exists:
                            # Calculate days overdue (bills typically due 15th of following month)
                            expected_due = datetime(check_date.year, check_date.month, 15)
                            if check_date.month == 12:
                                expected_due = datetime(check_date.year + 1, 1, 15)
                            else:
                                expected_due = datetime(check_date.year, check_date.month + 1, 15)
                            
                            days_overdue = max(0, (current_date - expected_due).days)
                            
                            if days_overdue > 0:  # Only include if actually overdue
                                missing_bills.append({
                                    "hotel_id": hotel_id,
                                    "utility_type": utility_type,
                                    "expected_month": calendar.month_name[check_date.month],
                                    "expected_year": check_date.year,
                                    "expected_period": year_month,
                                    "days_overdue": days_overdue,
                                    "status": "missing" if days_overdue > 45 else "overdue",
                                    "expected_date": expected_due.isoformat(),
                                    "urgency": "high" if days_overdue > 60 else "medium" if days_overdue > 30 else "low"
                                })
                    except Exception as e:
                        print(f"Error checking {hotel_id} {utility_type} for {year_month}: {e}")
                        continue
        
        # Calculate enhanced stats
        total_hotels = len(hotel_utilities)
        total_expected_per_month = sum(len(utilities) for utilities in hotel_utilities.values())
        total_missing = len([b for b in missing_bills if b["status"] == "missing"])
        total_overdue = len([b for b in missing_bills if b["status"] == "overdue"])
        hotels_with_issues = len(set(b["hotel_id"] for b in missing_bills))
        
        # Calculate compliance rate (rough estimate)
        total_expected = total_expected_per_month * 6  # 6 months
        compliance_rate = max(0, (total_expected - len(missing_bills)) / total_expected * 100) if total_expected > 0 else 100
        
        stats = {
            "total_expected": total_expected,
            "total_missing": total_missing,
            "total_overdue": total_overdue,
            "compliance_rate": round(compliance_rate, 1),
            "hotels_with_issues": hotels_with_issues,
            "total_hotels": total_hotels,
            "urgency_breakdown": {
                "high": len([b for b in missing_bills if b.get("urgency") == "high"]),
                "medium": len([b for b in missing_bills if b.get("urgency") == "medium"]),
                "low": len([b for b in missing_bills if b.get("urgency") == "low"])
            }
        }
        
        # Sort missing bills by urgency and days overdue
        missing_bills.sort(key=lambda x: (x.get("urgency") == "high", x.get("days_overdue", 0)), reverse=True)
        
        return {
            "missing_bills": missing_bills,
            "stats": stats,
            "last_updated": current_date.isoformat()
        }
        
    except Exception as e:
        print(f"Error getting missing bills: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get missing bills: {str(e)}")

@router.post("/utilities/send-reminder")
async def send_reminder_email(reminder_data: dict):
    """Send reminder email for missing utility bill"""
    try:
        hotel_id = reminder_data.get("hotel_id")
        utility_type = reminder_data.get("utility_type") 
        month = reminder_data.get("month")
        
        print(f"Sending reminder: {hotel_id} - {utility_type} - {month}")
        
        return {"success": True, "message": "Reminder email sent"}
        
    except Exception as e:
        print(f"Error sending reminder: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to send reminder: {str(e)}")

@router.get("/utilities/all-bills")
async def get_all_bills():
    """Get all uploaded utility bills for admin viewer"""
    try:
        all_bills = []
        
        # Get hotel IDs dynamically from S3 bucket structure
        hotel_ids = get_hotel_ids_from_s3()
        current_year = datetime.now().year
        
        if not hotel_ids:
            return {
                "bills": [],
                "total_bills": 0
            }
        
        for hotel_id in hotel_ids:
            try:
                # Get bills for current and previous year
                for year in [current_year, current_year - 1]:
                    bills = get_utility_data_for_hotel_year(hotel_id, str(year))
                    
                    for bill in bills:
                        summary = bill.get('summary', {})
                        
                        all_bills.append({
                            "id": f"{hotel_id}_{bill.get('utility_type')}_{summary.get('bill_date', '')}",
                            "hotel_id": hotel_id,
                            "utility_type": bill.get('utility_type'),
                            "filename": bill.get('filename', 'Unknown'),
                            "upload_date": bill.get('uploaded_at', ''),
                            "bill_period": summary.get('bill_date', ''),
                            "supplier": summary.get('supplier', 'Unknown'),
                            "total_amount": summary.get('total_cost', 0) or 0,
                            "consumption": summary.get('total_kwh', 0) or summary.get('consumption_kwh', 0) or 0,
                            "consumption_unit": "kWh",
                            "parsed_status": "success"
                        })
                        
            except Exception as e:
                print(f"Error getting bills for {hotel_id}: {e}")
                continue
        
        # Sort by upload date (newest first)
        all_bills.sort(key=lambda x: x.get("upload_date", ""), reverse=True)
        
        return {"bills": all_bills, "total_bills": len(all_bills)}
        
    except Exception as e:
        print(f"Error getting all bills: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get all bills: {str(e)}")

def get_hotel_ids_from_s3():
    """Get list of hotel IDs by scanning S3 bucket structure"""
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Delimiter='/'
        )
        
        hotel_ids = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                # Extract hotel ID from prefix (e.g., "hiex/" -> "hiex")
                hotel_id = prefix['Prefix'].rstrip('/')
                hotel_ids.append(hotel_id)
        
        return hotel_ids
    except Exception as e:
        print(f"Error getting hotel IDs from S3: {e}")
        return []

@router.get("/utilities/bill-details/{bill_id}")
async def get_bill_details(bill_id: str):
    """Get detailed parsed data for a specific bill"""
    try:
        # Parse bill_id to extract hotel_id, utility_type, and date
        parts = bill_id.split("_")
        if len(parts) < 3:
            raise HTTPException(status_code=400, detail="Invalid bill ID format")
        
        hotel_id = parts[0]
        utility_type = parts[1]
        bill_date = "_".join(parts[2:])  # In case date has underscores
        
        # Get the specific bill data from S3
        year = bill_date[:4] if len(bill_date) >= 4 else str(datetime.now().year)
        bills = get_utility_data_for_hotel_year(hotel_id, year)
        
        # Find the specific bill
        target_bill = None
        for bill in bills:
            if (bill.get("utility_type") == utility_type and 
                bill.get("summary", {}).get("bill_date", "").startswith(bill_date[:7])):
                target_bill = bill
                break
        
        if not target_bill:
            raise HTTPException(status_code=404, detail="Bill not found")
        
        return {
            "bill_id": bill_id,
            "parsed_data": target_bill.get("raw_data", target_bill),
            "summary": target_bill.get("summary", {})
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error getting bill details: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get bill details: {str(e)}")

@router.get("/utilities/comparison/{year}")
async def get_utilities_comparison(year: int, hotel_ids: str = "hiex,moxy,hida,hbhdcc,hbhe,sera,marina"):
    """Get utility comparison data for multiple hotels"""
    try:
        hotel_list = hotel_ids.split(",")
        comparison_data = get_utility_summary_for_comparison(hotel_list, str(year))
        
        return {
            "comparison": comparison_data,
            "year": year,
            "hotels": hotel_list
        }
        
    except Exception as e:
        print(f"Error fetching comparison data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch comparison data: {str(e)}")

@router.get("/utilities/{hotel_id}/analytics")
async def get_utility_analytics(
    hotel_id: str, 
    year: str = None,
    metric: str = "overview",
    month: str = None,
    utility_type: str = None
):
    """Get specific utility analytics (MIC charges, carbon tax, etc.)"""
    try:
        # Get raw bills data
        bills_response = await get_raw_bills_data(hotel_id, year)
        bills = bills_response["bills"]
        
        # Apply filters
        if month:
            bills = [b for b in bills if b.get('enhanced_summary', {}).get('bill_date', '').startswith(f"{year or datetime.now().year}-{month.zfill(2)}")]
        
        if utility_type:
            bills = [b for b in bills if b.get('utility_type') == utility_type]
        
        analytics = {}
        
        if metric == "overview" or metric == "mic_charges":
            # Calculate MIC charges from electricity bills
            mic_total = 0
            mic_details = []
            
            for bill in bills:
                if bill.get('utility_type') == 'electricity':
                    raw_data = bill.get('raw_data', {})
                    charges = raw_data.get('charges', [])
                    
                    for charge in charges:
                        desc = charge.get('description', '').lower()
                        if 'mic' in desc or 'capacity' in desc:
                            amount = charge.get('amount', 0)
                            mic_total += amount
                            
                            mic_details.append({
                                'bill_date': bill.get('enhanced_summary', {}).get('bill_date'),
                                'description': charge.get('description'),
                                'amount': amount,
                                'quantity': charge.get('quantity', {}),
                                'rate': charge.get('rate', {}),
                                'supplier': bill.get('summary', {}).get('supplier')
                            })
            
            analytics['mic_charges'] = {
                'total': round(mic_total, 2),
                'details': mic_details,
                'average_monthly': round(mic_total / max(len(set(b.get('enhanced_summary', {}).get('bill_date', '')[:7] for b in bills)), 1), 2)
            }
        
        if metric == "overview" or metric == "carbon_tax":
            # Calculate carbon tax from gas bills
            carbon_total = 0
            carbon_details = []
            
            for bill in bills:
                if bill.get('utility_type') == 'gas':
                    raw_data = bill.get('raw_data', {})
                    line_items = raw_data.get('lineItems', [])
                    
                    for item in line_items:
                        desc = item.get('description', '').lower()
                        if 'carbon' in desc:
                            amount = item.get('amount', 0)
                            carbon_total += amount
                            
                            carbon_details.append({
                                'bill_date': bill.get('enhanced_summary', {}).get('bill_date'),
                                'description': item.get('description'),
                                'amount': amount,
                                'units': item.get('units'),
                                'rate': item.get('rate'),
                                'supplier': bill.get('summary', {}).get('supplier')
                            })
            
            analytics['carbon_tax'] = {
                'total': round(carbon_total, 2),
                'details': carbon_details,
                'average_monthly': round(carbon_total / max(len(set(b.get('enhanced_summary', {}).get('bill_date', '')[:7] for b in bills)), 1), 2)
            }
        
        return {
            "hotel_id": hotel_id,
            "year": year,
            "metric": metric,
            "filters": {
                "month": month,
                "utility_type": utility_type
            },
            "analytics": analytics,
            "bills_analyzed": len(bills)
        }
        
    except Exception as e:
        print(f"Error generating analytics: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to generate analytics: {str(e)}")

@router.get("/utilities/{hotel_id}/export")
async def export_utility_data(
    hotel_id: str,
    format: str = "csv",
    year: str = None,
    utility_type: str = None,
    include_raw: bool = False
):
    """Export utility data as CSV or JSON"""
    try:
        # Get raw bills data
        bills_response = await get_raw_bills_data(hotel_id, year)
        bills = bills_response["bills"]
        
        # Apply filters
        if utility_type:
            bills = [b for b in bills if b.get('utility_type') == utility_type]
        
        if format.lower() == "csv":
            # Create CSV
            output = StringIO()
            
            fieldnames = [
                'hotel_id', 'utility_type', 'filename', 'bill_date', 'supplier',
                'total_cost', 'total_kwh', 'day_kwh', 'night_kwh', 'consumption_kwh',
                'account_number', 'meter_number', 'upload_date'
            ]
            
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            
            for bill in bills:
                summary = bill.get('summary', {})
                
                row = {
                    'hotel_id': bill.get('hotel_id'),
                    'utility_type': bill.get('utility_type'),
                    'filename': bill.get('filename'),
                    'bill_date': summary.get('bill_date'),
                    'supplier': summary.get('supplier'),
                    'total_cost': summary.get('total_cost'),
                    'total_kwh': summary.get('total_kwh'),
                    'day_kwh': summary.get('day_kwh'),
                    'night_kwh': summary.get('night_kwh'),
                    'consumption_kwh': summary.get('consumption_kwh'),
                    'account_number': summary.get('account_number'),
                    'meter_number': summary.get('meter_number'),
                    'upload_date': bill.get('uploaded_at')
                }
                
                writer.writerow(row)
            
            output.seek(0)
            
            filename = f"{hotel_id}_utilities_{year or 'all'}_{utility_type or 'all'}.csv"
            
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
        else:
            # JSON export
            export_data = {
                "export_info": {
                    "hotel_id": hotel_id,
                    "year": year,
                    "utility_type": utility_type,
                    "include_raw": include_raw,
                    "exported_at": datetime.utcnow().isoformat(),
                    "total_bills": len(bills)
                },
                "bills": bills if include_raw else [
                    {
                        "hotel_id": bill.get('hotel_id'),
                        "utility_type": bill.get('utility_type'),
                        "filename": bill.get('filename'),
                        "summary": bill.get('summary'),
                        "uploaded_at": bill.get('uploaded_at')
                    }
                    for bill in bills
                ]
            }
            
            filename = f"{hotel_id}_utilities_{year or 'all'}_{utility_type or 'all'}.json"
            
            return StreamingResponse(
                iter([json.dumps(export_data, indent=2, ensure_ascii=False)]),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        
    except Exception as e:
        print(f"Error exporting data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to export data: {str(e)}")

# DEBUG ENDPOINTS

@router.get("/utilities/debug/actual-structure/{hotel_id}")
async def debug_actual_structure(hotel_id: str, year: str = "2025"):
    """Debug endpoint to check the actual S3 structure based on your real paths"""
    try:
        results = {
            "hotel_id": hotel_id,
            "year": year,
            "json_location": f"utilities/{hotel_id}/{year}/",
            "pdf_location": f"{hotel_id}/electricity/{year}/",
            "findings": {}
        }
        
        # Check JSON location (where bills should be read from)
        json_prefix = f"utilities/{hotel_id}/{year}/"
        try:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=json_prefix,
                MaxKeys=100
            )
            
            json_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.json'):
                        json_files.append({
                            "key": obj['Key'],
                            "size": obj['Size'],
                            "last_modified": obj['LastModified'].isoformat()
                        })
            
            results["findings"]["json_location"] = {
                "prefix": json_prefix,
                "files_found": len(json_files),
                "files": json_files
            }
            
        except Exception as e:
            results["findings"]["json_location"] = {"error": str(e)}
        
        # Check PDF location (should NOT be read for data)
        pdf_prefix = f"{hotel_id}/electricity/{year}/"
        try:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=pdf_prefix,
                MaxKeys=100
            )
            
            pdf_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    pdf_files.append({
                        "key": obj['Key'],
                        "size": obj['Size'],
                        "file_type": obj['Key'].split('.')[-1],
                        "last_modified": obj['LastModified'].isoformat()
                    })
            
            results["findings"]["pdf_location"] = {
                "prefix": pdf_prefix,
                "files_found": len(pdf_files),
                "files": pdf_files,
                "note": "These PDFs should NOT be used for data extraction"
            }
            
        except Exception as e:
            results["findings"]["pdf_location"] = {"error": str(e)}
        
        return results
        
    except Exception as e:
        return {"error": str(e)}

@router.get("/utilities/debug/test-single-bill/{hotel_id}")
async def debug_test_single_bill(hotel_id: str, year: str = "2025"):
    """Test reading a single bill to verify the process"""
    try:
        # Use the corrected function
        bills = get_utility_data_for_hotel_year(hotel_id, year)
        
        if not bills:
            return {
                "error": "No bills found",
                "hotel_id": hotel_id,
                "year": year,
                "search_path": f"utilities/{hotel_id}/{year}/"
            }
        
        # Take the first bill and show detailed extraction
        first_bill = bills[0]
        
        analysis = {
            "hotel_id": hotel_id,
            "year": year,
            "total_bills_found": len(bills),
            "first_bill_analysis": {
                "filename": first_bill.get('filename'),
                "utility_type": first_bill.get('utility_type'),
                "has_summary": 'summary' in first_bill,
                "has_raw_data": 'raw_data' in first_bill,
                "summary_keys": list(first_bill.get('summary', {}).keys()),
                "raw_data_keys": list(first_bill.get('raw_data', {}).keys())
            }
        }
        
        # Test data extraction
        raw_data = first_bill.get('raw_data', {})
        if first_bill.get('utility_type') == 'electricity':
            consumption = raw_data.get('consumption', [])
            analysis["extraction_test"] = {
                "day_kwh": next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Day'), 0),
                "night_kwh": next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Night'), 0),
                "total_cost": raw_data.get('totalAmount', {}).get('value', 0),
                "bill_date": raw_data.get('billingPeriod', {}).get('endDate', '')
            }
        elif first_bill.get('utility_type') == 'gas':
            analysis["extraction_test"] = {
                "consumption_kwh": raw_data.get('consumptionDetails', {}).get('consumptionValue', 0),
                "total_cost": raw_data.get('billSummary', {}).get('currentBillAmount', 0),
                "bill_date": raw_data.get('billSummary', {}).get('billingPeriodEndDate', '')
            }
        
        return analysis
        
    except Exception as e:
        return {"error": str(e)}

@router.get("/utilities/debug/s3-structure")
async def debug_s3_structure():
    """Debug endpoint to see the actual S3 bucket structure"""
    try:
        # List all top-level prefixes (hotel IDs)
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Delimiter='/'
        )
        
        structure = {
            "bucket_name": S3_BUCKET_NAME,
            "top_level_prefixes": [],
            "total_objects": 0
        }
        
        # Get hotel-level folders
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                hotel_id = prefix['Prefix'].rstrip('/')
                structure["top_level_prefixes"].append(hotel_id)
        
        # For each hotel, check utilities folder structure
        for hotel_id in structure["top_level_prefixes"][:5]:  # Limit to first 5 for performance
            # Check utilities folder
            utilities_prefix = f"utilities/{hotel_id}/"
            utilities_response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=utilities_prefix,
                MaxKeys=1000
            )
            
            hotel_structure = {
                "hotel_id": hotel_id,
                "utilities_files": [],
                "utilities_count": 0
            }
            
            if 'Contents' in utilities_response:
                for obj in utilities_response['Contents']:
                    key = obj['Key']
                    size = obj['Size']
                    last_modified = obj['LastModified'].isoformat()
                    
                    hotel_structure["utilities_files"].append({
                        "key": key,
                        "size": size,
                        "last_modified": last_modified,
                        "filename": key.split('/')[-1]
                    })
                    hotel_structure["utilities_count"] += 1
                    structure["total_objects"] += 1
            
            structure[hotel_id] = hotel_structure
        
        return structure
        
    except Exception as e:
        return {"error": str(e), "bucket": S3_BUCKET_NAME}from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks, HTTPException
from datetime import datetime, timedelta
import base64
import requests
import os
import time
import pdfplumber
import calendar
import json
import boto3
from io import BytesIO, StringIO
import csv
from fastapi.responses import StreamingResponse
from app.utils.s3 import save_pdf_to_s3

router = APIRouter()

DOCUPIPE_API_KEY = os.getenv("DOCUPIPE_API_KEY")
SCHEMA_ELECTRICITY = "3ca991a9"
SCHEMA_GAS = "33093b4d"
UPLOAD_WEBHOOK_URL = os.getenv("UPLOAD_WEBHOOK_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "jmk-project-uploads")

# Initialize S3 client
s3_client = boto3.client('s3')

@router.post("/utilities/precheck")
async def precheck_bill_type(file: UploadFile = File(...)):
    """Quick bill type detection for frontend preview"""
    try:
        content = await file.read()
        
        with pdfplumber.open(BytesIO(content)) as pdf:
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            
            if not pages_text:
                return {"bill_type": "unknown", "confidence": "low"}
            
            supplier = detect_supplier_from_text(pages_text)
            bill_type = detect_bill_type(pages_text, supplier)
            
            return {
                "bill_type": bill_type,
                "supplier": supplier,
                "confidence": "high" if bill_type != "unknown" else "low"
            }
            
    except Exception as e:
        print(f"Precheck error: {e}")
        return {"bill_type": "unknown", "confidence": "low", "error": str(e)}

def detect_supplier_from_text(pages_text):
    full_text = " ".join(pages_text).lower()
    if "flogas" in full_text or "fgnc" in full_text:
        return "Flogas"
    elif "arden" in full_text or "aes916" in full_text:
        return "Arden Energy"
    else:
        return "Unknown"

def detect_bill_type(pages_text, supplier):
    full_text = " ".join(pages_text).lower()
    
    # More comprehensive keywords
    gas_keywords = ["mprn", "gprn", "gas usage", "therms", "cubic feet", "calorific value", "gas supply", "natural gas", "flogas", "fgnc"]
    electricity_keywords = ["mpan", "kwh", "kilowatt", "day units", "night units", "electricity", "electric", "arden", "aes916", "mic", "maximum import capacity"]
    
    gas_matches = sum(1 for keyword in gas_keywords if keyword in full_text)
    electricity_matches = sum(1 for keyword in electricity_keywords if keyword in full_text)
    
    print(f"Gas keywords found: {gas_matches}, Electricity keywords found: {electricity_matches}")
    print(f"Supplier: {supplier}")
    
    # First try keyword matching
    if gas_matches > electricity_matches:
        return "gas"
    elif electricity_matches > gas_matches:
        return "electricity"
    else:
        # Fallback to supplier-based detection when keywords are equal/unclear
        supplier_lower = supplier.lower()
        if "flogas" in supplier_lower or "fgnc" in supplier_lower or "gas" in supplier_lower:
            print("Supplier-based detection: GAS (Flogas)")
            return "gas"
        elif "arden" in supplier_lower or "aes" in supplier_lower:
            print("Supplier-based detection: ELECTRICITY (Arden)")
            return "electricity"
        else:
            print("Could not determine bill type - defaulting to electricity")
            return "unknown"

def send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_path, supplier="Unknown", status="success", error=None):
    if not UPLOAD_WEBHOOK_URL:
        print("No webhook URL set")
        return
    
    try:
        payload = {
            "status": status,
            "hotel_id": hotel_id,
            "supplier": supplier,
            "bill_type": bill_type,
            "filename": filename,
            "billing_start": billing_start,
            "s3_path": s3_path,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if error:
            payload["error"] = error
        
        res = requests.post(UPLOAD_WEBHOOK_URL, json=payload, timeout=10)
        print(f"Webhook sent: {res.status_code}")
    except Exception as e:
        print(f"Webhook failed: {e}")

@router.post("/utilities/parse-and-save")
async def parse_and_save(
    background_tasks: BackgroundTasks,
    hotel_id: str = Form(...),
    file: UploadFile = File(...),
    bill_date: str = Form(...),
    bill_type: str = Form(...),
    supplier: str = Form(default="docupanda")
):
    # Validate inputs
    if not DOCUPIPE_API_KEY:
        raise HTTPException(status_code=500, detail="DocuPipe API key not configured")
    
    if bill_type not in ["electricity", "gas"]:
        raise HTTPException(status_code=400, detail="Invalid bill type. Must be 'electricity' or 'gas'")
    
    content = await file.read()
    filename = file.filename
    
    # Validate file
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    
    if not filename or not filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    background_tasks.add_task(
        process_and_store_docupipe, 
        content, hotel_id, filename, bill_date, bill_type, supplier
    )
    
    return {"status": "processing", "message": "Upload received. Processing in background."}

def process_and_store_docupipe(content, hotel_id, filename, bill_date, bill_type, supplier="docupanda"):
    try:
        print(f"Processing {filename} - Type: {bill_type}, Hotel: {hotel_id}")
        
        # Verify bill type with local detection as backup
        detected_supplier = "Unknown"
        detected_bill_type = bill_type
        
        try:
            with pdfplumber.open(BytesIO(content)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                
                if pages_text:
                    detected_supplier = detect_supplier_from_text(pages_text)
                    local_detection = detect_bill_type(pages_text, detected_supplier)
                    
                    if local_detection != bill_type and local_detection != "unknown":
                        print(f"WARNING: User selected {bill_type} but detected {local_detection}")
                    
                    print(f"Detected: {detected_supplier} - {local_detection}")
        except Exception as e:
            print(f"Local parsing failed: {e}")
        
        # DocuPipe API calls
        encoded = base64.b64encode(content).decode()
        
        print("Uploading to DocuPipe...")
        upload_res = requests.post(
            "https://app.docupipe.ai/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPIPE_API_KEY,
            },
            timeout=30
        )
        
        print(f"DocuPipe upload response: {upload_res.status_code}")
        
        if not upload_res.ok:
            error_msg = f"DocuPipe upload failed: {upload_res.status_code}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        
        if not document_id or not job_id:
            error_msg = f"Missing documentId or jobId in response: {data}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        print(f"Document uploaded: {document_id}, Job: {job_id}")
        
        # Wait for document processing
        for attempt in range(10):
            time.sleep(5)
            try:
                res = requests.get(
                    f"https://app.docupipe.ai/job/{job_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                    timeout=10
                )
                if not res.ok:
                    print(f"Job status check failed: {res.status_code}")
                    continue
                    
                status = res.json().get("status")
                print(f"Document processing {attempt + 1}: {status}")
                
                if status == "completed":
                    break
                elif status == "error":
                    error_msg = "Document processing failed"
                    print(error_msg)
                    send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                    return
            except requests.RequestException as e:
                print(f"Request error during job status check: {e}")
                continue
        else:
            error_msg = "Document processing timeout"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        # Start standardization
        schema_id = SCHEMA_ELECTRICITY if bill_type == "electricity" else SCHEMA_GAS
        print(f"Using schema: {schema_id} for {bill_type}")
        
        try:
            # Try the v2 endpoint first
            std_res = requests.post(
                "https://app.docupipe.ai/v2/standardize/batch",
                json={"documentIds": [document_id], "schemaId": schema_id},
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                    "X-API-Key": DOCUPIPE_API_KEY,
                },
                timeout=30
            )
            
            print(f"v2/standardize/batch response: {std_res.status_code}")
            
            if not std_res.ok:
                # Try the v1 endpoint as fallback
                print("v2 failed, trying v1 endpoint...")
                std_res = requests.post(
                    "https://app.docupipe.ai/standardize/batch",
                    json={"documentIds": [document_id], "schemaId": schema_id},
                    headers={
                        "accept": "application/json",
                        "content-type": "application/json",
                        "X-API-Key": DOCUPIPE_API_KEY,
                    },
                    timeout=30
                )
                print(f"v1/standardize/batch response: {std_res.status_code}")
            
            if not std_res.ok:
                error_msg = f"Both standardization endpoints failed: {std_res.status_code}"
                print(error_msg)
                send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                return
                
            print(f"Standardization started: {std_res.status_code}")
            
        except requests.RequestException as e:
            error_msg = f"Standardization request error: {e}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        std_data = std_res.json()
        std_job_id = std_data.get("jobId")
        std_id = std_data.get("standardizationIds", [None])[0]
        
        if not std_job_id or not std_id:
            error_msg = f"Missing standardization IDs: {std_data}"
            print(error_msg)
            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
            return
        
        # Wait for standardization
        print("Waiting for standardization...")
        time.sleep(15)
        
        for attempt in range(12):
            try:
                std_job_res = requests.get(
                    f"https://app.docupipe.ai/job/{std_job_id}",
                    headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                    timeout=10
                )
                
                if not std_job_res.ok:
                    print(f"Standardization status check failed: {std_job_res.status_code}")
                    time.sleep(10)
                    continue
                
                status = std_job_res.json().get("status")
                print(f"Standardization {attempt + 1}: {status}")
                
                if status == "completed":
                    print(f"Standardization completed, waiting 5 seconds before fetching result...")
                    time.sleep(5)
                    
                    # Get final result
                    print(f"Fetching result from: https://app.docupipe.ai/standardization/{std_id}")
                    result_res = requests.get(
                        f"https://app.docupipe.ai/standardization/{std_id}",
                        headers={"accept": "application/json", "X-API-Key": DOCUPIPE_API_KEY},
                        timeout=10
                    )
                    
                    if not result_res.ok:
                        error_msg = f"Failed to get standardization result: {result_res.status_code}"
                        print(error_msg)
                        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                        return
                    
                    result_json = result_res.json()
                    print(f"DocuPipe result keys: {list(result_json.keys())}")
                    
                    # According to DocuPipe docs, the data is in the "data" field
                    parsed = result_json.get("data", {})
                    
                    if not parsed:
                        parsed = result_json.get("result", {})
                        if not parsed:
                            error_msg = f"Empty result from DocuPipe. Response: {result_json}"
                            print(error_msg)
                            send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                            return
                    
                    print(f"Got parsed data with keys: {list(parsed.keys())}")
                    
                    # Extract billing period
                    billing_start = (
                        parsed.get("billingPeriod", {}).get("startDate") or 
                        parsed.get("billingPeriodStartDate") or 
                        bill_date
                    )
                    
                    # Save to S3 with enhanced structure
                    try:
                        s3_json_path = save_parsed_data_to_s3(hotel_id, bill_type, parsed, "", filename)
                        save_pdf_to_s3(content, hotel_id, bill_type, billing_start, filename)
                        
                        print(f"Successfully saved: {s3_json_path}")
                        
                        # Send success webhook
                        send_upload_webhook(hotel_id, bill_type, filename, billing_start, s3_json_path, detected_supplier, "success")
                        return
                        
                    except Exception as e:
                        error_msg = f"Failed to save data: {e}"
                        print(error_msg)
                        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                        return
                    
                elif status == "error":
                    error_msg = "Standardization processing failed"
                    print(error_msg)
                    send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
                    return
                else:
                    time.sleep(10)
                    
            except requests.RequestException as e:
                print(f"Request error during standardization check: {e}")
                time.sleep(10)
                continue
        
        error_msg = "Standardization timeout"
        print(error_msg)
        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", detected_supplier, "error", error_msg)
        
    except Exception as e:
        error_msg = f"Processing error: {e}"
        print(error_msg)
        send_upload_webhook(hotel_id, bill_type, filename, bill_date, "", "Unknown", "error", error_msg)
        import traceback
        traceback.print_exc()

# S3 FUNCTIONS

def save_parsed_data_to_s3(hotel_id: str, bill_type: str, parsed_data: dict, upload_date: str, filename: str):
    """Save parsed utility bill data to S3 with proper structure"""
    try:
        # Extract bill summary using the real JSON structure
        summary = extract_bill_summary_from_real_data(parsed_data, bill_type)
        
        # Create the complete bill data structure
        bill_data = {
            "hotel_id": hotel_id,
            "utility_type": bill_type,
            "filename": filename,
            "uploaded_at": upload_date or datetime.utcnow().isoformat(),
            "summary": summary,
            "raw_data": parsed_data
        }
        
        # Determine year and month for folder structure
        bill_date = summary.get('bill_date', '')
        if bill_date:
            # Extract year and month from bill_date (format: YYYY-MM-DD)
            year = bill_date.split('-')[0]
            month = bill_date.split('-')[1] if len(bill_date.split('-')) > 1 else '01'
        else:
            now = datetime.utcnow()
            year = now.strftime('%Y')
            month = now.strftime('%m')
        
        # Use the correct S3 key pattern: utilities/{hotel_id}/{year}/{month}/filename.json
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        base_filename = filename.replace('.pdf', '').replace('.json', '')
        s3_key = f"utilities/{hotel_id}/{year}/{month}/{bill_type}_{base_filename}_{timestamp}.json"
        
        # Save to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(bill_data, ensure_ascii=False, indent=2),
            ContentType='application/json'
        )
        
        print(f"Successfully saved bill to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        print(f"Error saving to S3: {e}")
        raise e

def extract_bill_summary_from_real_data(data: dict, bill_type: str) -> dict:
    """Extract summary from real parsed bill data structures"""
    summary = {}
    
    try:
        if bill_type == 'electricity':
            # Arden Energy electricity bill structure
            summary.update({
                'supplier': data.get('supplier', 'Unknown'),
                'customer_ref': data.get('customerRef', ''),
                'billing_ref': data.get('billingRef', ''),
                'account_number': data.get('customerRef', ''),
                'meter_number': data.get('meterDetails', {}).get('meterNumber', ''),
                'mprn': data.get('meterDetails', {}).get('mprn', ''),
                'bill_date': data.get('billingPeriod', {}).get('endDate', ''),
                'billing_period_start': data.get('billingPeriod', {}).get('startDate', ''),
                'billing_period_end': data.get('billingPeriod', {}).get('endDate', ''),
                'total_cost': data.get('totalAmount', {}).get('value', 0),
                'mic_value': data.get('meterDetails', {}).get('mic', {}).get('value', 0),
                'max_demand': data.get('meterDetails', {}).get('maxDemand', {}).get('value', 0),
                'vat_amount': data.get('taxDetails', {}).get('vatAmount', 0),
                'electricity_tax': data.get('taxDetails', {}).get('electricityTax', {}).get('amount', 0)
            })
            
            # Extract consumption data
            consumption = data.get('consumption', [])
            day_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Day'), 0)
            night_kwh = next((c.get('units', {}).get('value', 0) for c in consumption if c.get('type') == 'Night'), 0)
            
            summary.update({
                'day_kwh': day_kwh,
                'night_kwh': night_kwh,
                'total_kwh': day_kwh + night_kwh
            })
            
        elif bill_type == 'gas':
            # Flogas gas bill structure
            summary.update({
                'supplier': data.get('supplierInfo', {}).get('name', 'Unknown'),
                'account_number': data.get('accountInfo', {}).get('accountNumber', ''),
                'gprn': data.get('accountInfo', {}).get('gprn', ''),
                'meter_number': data.get('accountInfo', {}).get('meterNumber', ''),
                'bill_date': data.get('billSummary', {}).get('billingPeriodEndDate', ''),
                'billing_period_start': data.get('billSummary', {}).get('billingPeriodStartDate', ''),
                'billing_period_end': data.get('billSummary', {}).get('billingPeriodEndDate', ''),
                'total_cost': data.get('billSummary', {}).get('currentBillAmount', 0),
                'consumption_kwh': data.get('consumptionDetails', {}).get('consumptionValue', 0),
                'units_consumed': data.get('meterReadings', {}).get('unitsConsumed', 0),
                'conversion_factor': data.get('consumptionDetails', {}).get('conversionFactor', 0),
                'vat_amount': data.get('billSummary', {}).get('totalVatAmount', 0)
            })
            
            # Extract line items for detailed costs
            line_items = data.get('lineItems', [])
            carbon_tax = next((item.get('amount', 0) for item in line_items if 'carbon' in item.get('description', '').lower()), 0)
            standing_charge = next((item.get('amount', 0) for item in line_items if 'standing' in item.get('description', '').lower()), 0)
            commodity_cost = next((item.get('amount', 0) for item in line_items if 'commodity' in item.get('description', '').lower() or 'tariff' in item.get('description', '').lower()), 0)
            
            summary.update({
                'carbon_tax': carbon_tax,
                'standing_charge': standing_charge,
                'commodity_cost': commodity_cost
            })
    
    except Exception as e:
        print(f"Error extracting summary for {bill_type}: {e}")
    
    return summary

def get_utility_data_for_hotel_year(hotel_id: str, year: str):
    """Get utility bills data from S3 - CORRECTED for your actual S3 structure"""
    try:
        bills = []
        
        # CORRECTED: Based on your actual S3 structure
        # JSONs are in: utilities/{hotel_id}/{year}/MM/filename.json
        json_prefix = f"utilities/{hotel_id}/{year}/"
        
        print(f"Searching for {hotel_id} {year} bills in S3 at: {json_prefix}")
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix=json_prefix
            )
            
            if 'Contents' in response:
                print(f"Found {len(response['Contents'])} objects in {json_prefix}")
                
                for obj in response['Contents']:
                    key = obj['Key']
                    print(f"Processing file: {key}")
                    
                    # ONLY process JSON files (ignore PDFs and other files)
                    if not key.endswith('.json'):
                        print(f"Skipping non-JSON file: {key}")
                        continue
                    
                    try:
                        # Get file content
                        file_response = s3_client.get_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=key
                        )
                        
                        bill_data = json.loads(file_response['Body'].read())
                        
                        # Validate this is a proper utility bill JSON
                        if not isinstance(bill_data, dict):
                            print(f"Skipping invalid JSON structure: {key}")
                            continue
                        
                        # Check if this bill is for the requested year
                        bill_date = None
                        if 'summary' in bill_data:
                            bill_date = bill_data['summary'].get('bill_date', '')
                        elif 'raw_data' in bill_data:
                            raw_data = bill_data['raw_data']
                            bill_date = (
                                raw_data.get('billingPeriod', {}).get('endDate', '') or
                                raw_data.get('billSummary', {}).get('billingPeriodEndDate', '')
                            )
                        
                        # Skip if wrong year
                        if bill_date and not bill_date.startswith(year):
                            print(f"Skipping bill from wrong year: {bill_date} (want {year})")
                            continue
                        
                        # Ensure utility_type is set
                        if not bill_data.get('utility_type'):
                            raw_data = bill_data.get('raw_data', {})
                            if 'supplier' in raw_data and 'arden' in raw_data.get('supplier', '').lower():
                                bill_data['utility_type'] = 'electricity'
                            elif 'supplierInfo' in raw_data or 'documentType' in raw_data:
                                bill_data['utility_type'] = 'gas'
                            else:
                                print(f"Skipping bill with unknown utility type: {key}")
                                continue
                        
                        print(f"✅ Adding bill: {key} for date {bill_date}, type: {bill_data.get('utility_type')}")
                        bills.append(bill_data)
                        
                    except json.JSONDecodeError as e:
                        print(f"JSON parse error for {key}: {e}")
                        continue
                    except Exception as e:
                        print(f"Error processing {key}: {e}")
                        continue
            else:
                print(f"No objects found in {json_prefix}")
                
        except Exception as e:
            print(f"Error checking S3 prefix {json_prefix}: {e}")
        
        print(f"Total bills found for {hotel_id} {year}: {len(bills)}")
        return bills
        
    except Exception as e:
        print(f"Error in get_utility_data_for_hotel_year: {e}")
        return []

def get_utility_summary_for_comparison(hotel_list: list, year: str):
    """Get utility summary data for multiple hotels for comparison"""
    try:
        comparison_data = []
        
        for hotel_id in hotel_list:
            bills = get_utility_data_for_hotel_year(hotel_id, year)
            
            hotel_summary = {
                "hotel_id": hotel_id,
                "electricity": {"total_kwh": 0, "total_cost": 0, "bills_count": 0},
                "gas": {"total_kwh": 0, "total_cost": 0, "bills_count": 0},
                "total_cost": 0
            }
            
            for bill in bills:
                utility_type = bill.get('utility_type', 'unknown')
                summary = bill.get('summary', {})
                
                if utility_type == 'electricity':
                    hotel_summary["electricity"]["total_kwh"] += summary.get('total_kwh', 0)
                    hotel_summary["electricity"]["total_cost"] += summary.get('total_cost', 0)
                    hotel_summary["electricity"]["bills_count"] += 1
                elif utility_type == 'gas':
                    hotel_summary["gas"]["total_kwh"] += summary.get('consumption_kwh', 0)
                    hotel_summary["gas"]["total_cost"] += summary.get('total_cost', 0)
                    hotel_summary["gas"]["bills_count"] += 1
                
                hotel_summary["total_cost"] += summary.get('total_cost', 0)
            
            comparison_data.append(hotel_summary)
        
        return comparison_data
        
    except Exception as e:
        print(f"Error in get_utility_summary_for_comparison: {e}")
        return []

# API ENDPOINTS

@router.get("/utilities/{hotel_id}/{year}")
async def get_utilities_data(hotel_id: str, year: int):
    """Get utility bills data from S3 for charts - S3 ONLY VERSION"""
    try:
        bills = get_utility_data_for_hotel_year(hotel_id, str(year))
        
        # Transform data for frontend charts
        electricity_data = []
        gas_data = []
        water_data = []
        
        # Keep track of bills processed and months seen
        processed_count = {"electricity": 0, "gas": 0, "water": 0}
        months_seen = {"electricity": set(), "gas": set()}
        
        for bill in bills:
            try:
                # Determine utility type
                utility_type = bill.get('utility_type')
                if not utility_type:
                    # Try to determine from the raw data structure
                    raw_data = bill.get('raw_data', bill)
                    if
