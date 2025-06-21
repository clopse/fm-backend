import anthropic
import base64
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any, Set
import pdf2image
from PIL import Image
import io
import calendar
import math
import pdfplumber
from decimal import Decimal, ROUND_HALF_UP

class SmartBillExtractor:
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.tolerance = 0.01  # Much stricter tolerance - we want DocuPipe accuracy
        
    def extract_bill_data(self, pdf_content: bytes, bill_type: str, filename: str) -> Dict:
        """Extract with precision and intelligence"""
        try:
            print(f"🧠 SmartBillExtractor processing {filename}")
            
            # PHASE 1: Comprehensive PDF analysis
            pdf_intelligence = self._comprehensive_pdf_analysis(pdf_content, bill_type)
            print(f"📊 PDF Analysis: Found {len(pdf_intelligence['all_numbers'])} numbers, {len(pdf_intelligence['tables'])} tables")
            
            # PHASE 2: Multi-shot Claude extraction with validation
            claude_result = self._multi_shot_claude_extraction(pdf_content, pdf_intelligence, bill_type)
            print(f"🤖 Claude extraction completed with {claude_result.get('confidence', 0)}% initial confidence")
            
            # PHASE 3: DocuPipe-level validation and correction
            final_data = self._docupipe_level_validation(claude_result, pdf_intelligence, bill_type)
            print(f"✅ Final validation: {len(final_data.get('_validation', {}).get('corrections', []))} corrections made")
            
            # PHASE 4: Quality assurance check
            quality_report = self._quality_assurance_check(final_data, pdf_intelligence, bill_type)
            
            return {
                "raw_data": final_data,
                "validation": quality_report,
                "extraction_method": "smart_bill_extractor",
                "pdf_stats": {
                    "numbers_found": len(pdf_intelligence['all_numbers']),
                    "tables_found": len(pdf_intelligence['tables']),
                    "supplier": pdf_intelligence.get('supplier_detected', 'Unknown')
                }
            }
            
        except Exception as e:
            print(f"❌ SmartBillExtractor error: {e}")
            raise e
    
    def _comprehensive_pdf_analysis(self, pdf_content: bytes, bill_type: str) -> Dict:
        """Ultra-comprehensive PDF analysis to extract all possible data"""
        
        analysis = {
            "full_text": "",
            "all_numbers": [],
            "number_contexts": {},
            "tables": [],
            "supplier_detected": "Unknown",
            "bill_patterns": {},
            "field_candidates": {}
        }
        
        try:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                full_text = ""
                all_tables = []
                
                for page_num, page in enumerate(pdf.pages):
                    # Extract text
                    page_text = page.extract_text() or ""
                    full_text += page_text + "\n"
                    
                    # Extract tables with maximum detail
                    tables = page.extract_tables()
                    for table_idx, table in enumerate(tables):
                        if table and len(table) > 1:
                            structured_table = self._analyze_table_structure(table, bill_type)
                            all_tables.append({
                                'page': page_num,
                                'table_index': table_idx,
                                'raw_table': table,
                                'structured': structured_table,
                                'confidence': structured_table.get('confidence', 0)
                            })
                
                analysis['full_text'] = full_text
                analysis['tables'] = all_tables
                
                # Advanced supplier detection
                analysis['supplier_detected'] = self._advanced_supplier_detection(full_text)
                
                # Extract all numbers with precise context
                analysis.update(self._extract_numbers_with_precision(full_text))
                
                # Detect bill patterns specific to supplier
                analysis['bill_patterns'] = self._detect_bill_patterns(full_text, analysis['supplier_detected'], bill_type)
                
                # Pre-identify likely field values
                analysis['field_candidates'] = self._identify_field_candidates(full_text, all_tables, bill_type)
                
        except Exception as e:
            print(f"PDF analysis error: {e}")
        
        return analysis
    
    def _analyze_table_structure(self, table: List[List], bill_type: str) -> Dict:
        """Analyze table structure with DocuPipe-level intelligence"""
        
        structure = {
            "headers": [],
            "data_rows": [],
            "charge_rows": [],
            "consumption_rows": [],
            "numbers_by_column": {},
            "confidence": 0
        }
        
        if not table or len(table) < 2:
            return structure
        
        # Clean and analyze headers
        headers = [str(cell or '').strip() for cell in table[0]]
        structure['headers'] = headers
        
        # Analyze each data row
        for row_idx, row in enumerate(table[1:], 1):
            if not row or not any(cell for cell in row):
                continue
                
            clean_row = [str(cell or '').strip() for cell in row]
            structure['data_rows'].append(clean_row)
            
            # Categorize row type
            row_text = ' '.join(clean_row).lower()
            
            # Check if this is a charge/line item row
            if self._is_charge_row(row_text, bill_type):
                parsed_charge = self._parse_charge_row(clean_row, headers, bill_type)
                if parsed_charge:
                    structure['charge_rows'].append(parsed_charge)
            
            # Check if this is consumption data
            if bill_type == 'electricity' and self._is_consumption_row(row_text):
                parsed_consumption = self._parse_consumption_row(clean_row, headers)
                if parsed_consumption:
                    structure['consumption_rows'].append(parsed_consumption)
            
            # Extract numbers by column position
            for col_idx, cell in enumerate(clean_row):
                numbers = re.findall(r'\d+\.?\d*', cell)
                for num_str in numbers:
                    try:
                        num_val = float(num_str)
                        if col_idx not in structure['numbers_by_column']:
                            structure['numbers_by_column'][col_idx] = []
                        structure['numbers_by_column'][col_idx].append(num_val)
                    except ValueError:
                        continue
        
        # Calculate confidence based on how much structured data we found
        structure['confidence'] = min(100, 
                                    len(structure['charge_rows']) * 25 + 
                                    len(structure['consumption_rows']) * 20 + 
                                    (10 if headers else 0))
        
        return structure
    
    def _is_charge_row(self, row_text: str, bill_type: str) -> bool:
        """Determine if table row contains charge/line item data"""
        charge_indicators = [
            'charge', 'units', 'rate', 'amount', 'tariff', 'cost', 'fee', 
            'standing', 'capacity', 'commodity', 'carbon', 'tax', 'levy'
        ]
        
        if bill_type == 'electricity':
            charge_indicators.extend(['day units', 'night units', 'kwh', 'mic', 'pso'])
        else:  # gas
            charge_indicators.extend(['commodity', 'shrinkage', 'transmission', 'distribution'])
        
        return any(indicator in row_text for indicator in charge_indicators)
    
    def _is_consumption_row(self, row_text: str) -> bool:
        """Determine if row contains consumption data"""
        consumption_indicators = ['day', 'night', 'kwh', 'units', 'consumption', 'wattless']
        return any(indicator in row_text for indicator in consumption_indicators)
    
    def _parse_charge_row(self, row: List[str], headers: List[str], bill_type: str) -> Optional[Dict]:
        """Parse a charge row into structured data"""
        try:
            charge = {
                "description": "",
                "quantity": None,
                "rate": None,
                "amount": None,
                "units": "",
                "confidence": 0
            }
            
            # Find description (usually first non-numeric column)
            for cell in row:
                if cell and not re.match(r'^\d+\.?\d*$', cell.strip()):
                    if not charge["description"] or len(cell) > len(charge["description"]):
                        charge["description"] = cell.strip()
            
            # Extract numbers from row
            numbers = []
            for cell in row:
                if cell:
                    cell_numbers = re.findall(r'\d+\.?\d*', cell)
                    for num_str in cell_numbers:
                        try:
                            numbers.append(float(num_str))
                        except ValueError:
                            continue
            
            # Intelligently assign numbers based on magnitude and context
            if len(numbers) >= 3:  # quantity, rate, amount
                numbers.sort()
                
                # Amount is usually the largest number
                charge["amount"] = numbers[-1]
                
                # Rate is usually a small decimal
                rates = [n for n in numbers if 0.001 <= n <= 10]
                if rates:
                    charge["rate"] = rates[0]
                
                # Quantity is what's left
                quantities = [n for n in numbers if n not in [charge["amount"], charge["rate"]]]
                if quantities:
                    charge["quantity"] = quantities[-1]  # Usually largest remaining
            
            elif len(numbers) == 2:  # likely rate and amount
                if any(n < 1 for n in numbers):  # One is likely a rate
                    charge["rate"] = min(numbers)
                    charge["amount"] = max(numbers)
                else:
                    charge["quantity"] = min(numbers)
                    charge["amount"] = max(numbers)
            
            # Confidence based on how complete the extraction is
            fields_found = sum(1 for field in [charge["description"], charge["amount"]] if field)
            charge["confidence"] = (fields_found / 2) * 100
            
            return charge if charge["confidence"] > 50 else None
            
        except Exception as e:
            print(f"Error parsing charge row: {e}")
            return None
    
    def _parse_consumption_row(self, row: List[str], headers: List[str]) -> Optional[Dict]:
        """Parse consumption row for electricity bills"""
        try:
            consumption = {
                "type": "",
                "units": None,
                "confidence": 0
            }
            
            # Determine consumption type
            row_text = ' '.join(row).lower()
            if 'day' in row_text:
                consumption["type"] = "Day"
            elif 'night' in row_text:
                consumption["type"] = "Night"
            elif 'wattless' in row_text:
                consumption["type"] = "Wattless"
            
            # Extract consumption value (usually largest number)
            numbers = []
            for cell in row:
                if cell:
                    cell_numbers = re.findall(r'\d+', cell)
                    for num_str in cell_numbers:
                        try:
                            num = int(num_str)
                            if 1 <= num <= 1000000:  # Reasonable consumption range
                                numbers.append(num)
                        except ValueError:
                            continue
            
            if numbers:
                consumption["units"] = max(numbers)  # Usually the largest number
                consumption["confidence"] = 90 if consumption["type"] else 70
            
            return consumption if consumption["confidence"] > 60 else None
            
        except Exception as e:
            print(f"Error parsing consumption row: {e}")
            return None
    
    def _multi_shot_claude_extraction(self, pdf_content: bytes, pdf_intelligence: Dict, bill_type: str) -> Dict:
        """Multi-shot Claude extraction with iterative refinement"""
        
        # Convert all pages to high-quality images
        images = pdf2image.convert_from_bytes(pdf_content, dpi=300)  # Higher DPI for better OCR
        image_data = self._convert_images_to_base64(images)
        
        # SHOT 1: Initial extraction with comprehensive prompt
        initial_prompt = self._create_precision_prompt(pdf_intelligence, bill_type)
        initial_extraction = self._claude_extraction_shot(image_data, initial_prompt)
        
        # SHOT 2: Validation and refinement if needed
        validation_issues = self._quick_validation_check(initial_extraction, pdf_intelligence, bill_type)
        
        if validation_issues:
            print(f"🔍 Found {len(validation_issues)} issues, running refinement shot")
            refinement_prompt = self._create_refinement_prompt(initial_extraction, validation_issues, pdf_intelligence, bill_type)
            refined_extraction = self._claude_extraction_shot(image_data, refinement_prompt)
            return refined_extraction
        
        return initial_extraction
    
    def _create_precision_prompt(self, pdf_intelligence: Dict, bill_type: str) -> str:
        """Create ultra-detailed prompt for maximum accuracy"""
        
        base_schema = self._get_exact_schema(bill_type)
        
        intelligence_context = f"""
        CONTEXT FROM PDF ANALYSIS:
        - Supplier detected: {pdf_intelligence.get('supplier_detected', 'Unknown')}
        - Numbers found in PDF: {pdf_intelligence.get('all_numbers', [])[:30]}
        - Table structures found: {len(pdf_intelligence.get('tables', []))}
        - Bill patterns detected: {pdf_intelligence.get('bill_patterns', {})}
        
        PRE-IDENTIFIED FIELD CANDIDATES:
        {json.dumps(pdf_intelligence.get('field_candidates', {}), indent=2)}
        """
        
        precision_instructions = """
        CRITICAL PRECISION REQUIREMENTS:
        
        1. DECIMAL PRECISION: Pay extreme attention to decimal places
           - Rate 0.048700 is NOT the same as 0.48700
           - Always include trailing zeros if shown: 0.048700 not 0.0487
           - Be very careful with rates - they're often small decimals
        
        2. METER READINGS: Handle estimates and actuals
           - Look for 'E' (Estimate), 'A' (Actual), 'C' (Customer Read)
           - Extract the number: "296551 E" becomes "296551" with note it's estimated
        
        3. DATE PRECISION: 
           - Always use YYYY-MM-DD format
           - "July 2024" becomes "2024-07-01" to "2024-07-31"
           - Handle period descriptions: "April 2024" = "2024-04-01" to "2024-04-30"
        
        4. CONSUMPTION PRECISION:
           - Day/Night/Wattless must be exact integers
           - Double-check consumption totals add up
        
        5. CHARGE DESCRIPTIONS:
           - Use exact wording from bill: "Standing Charge" not "standing charge"
           - Preserve special characters and spacing
        
        6. VAT CALCULATIONS:
           - Ireland electricity VAT = 9%
           - Ireland gas VAT = 13.5% 
           - Verify VAT calculations are mathematically correct
        """
        
        return f"""
        You are an expert bill data extractor with industry-leading accuracy.
        Extract ALL data from this {bill_type} bill into the EXACT JSON structure below.
        
        {base_schema}
        
        {intelligence_context}
        
        {precision_instructions}
        
        VALIDATION REQUIREMENTS:
        - Every number you extract MUST appear in the PDF numbers list above
        - All mathematical relationships must be correct
        - Cross-reference table data with your extractions
        - If unsure about any field, use null rather than guessing
        
        Extract with absolute precision. Lives depend on this accuracy.
        """
    
    def _get_exact_schema(self, bill_type: str) -> str:
        """Get the exact schema format we need"""
        
        if bill_type == "electricity":
            return '''
            ELECTRICITY BILL JSON SCHEMA (Required format):
            ```json
            {
                "supplier": "string",
                "customerRef": "string",
                "billingRef": "string", 
                "billingPeriod": {
                    "startDate": "YYYY-MM-DD",
                    "endDate": "YYYY-MM-DD"
                },
                "customer": {
                    "name": "string",
                    "address": {
                        "street": "string",
                        "city": "string",
                        "postalCode": "string"
                    }
                },
                "meterDetails": {
                    "mprn": "string",
                    "meterNumber": "string", 
                    "meterType": "string",
                    "mic": {
                        "value": number,
                        "unit": "kVa"
                    },
                    "maxDemand": {
                        "value": number,
                        "unit": "kVa"
                    },
                    "maxDemandDate": "YYYY-MM-DD"
                },
                "consumption": [
                    {
                        "type": "Day",
                        "units": {
                            "value": number,
                            "unit": "kWh"
                        }
                    },
                    {
                        "type": "Night",
                        "units": {
                            "value": number,
                            "unit": "kWh"
                        }
                    },
                    {
                        "type": "Wattless",
                        "units": {
                            "value": number,
                            "unit": "kWh"
                        }
                    }
                ],
                "charges": [
                    {
                        "description": "string",
                        "quantity": {
                            "value": number,
                            "unit": "string"
                        },
                        "rate": {
                            "value": number,
                            "unit": "string"
                        },
                        "amount": number
                    }
                ],
                "taxDetails": {
                    "vatRate": number,
                    "vatAmount": number,
                    "electricityTax": {
                        "quantity": {
                            "value": number,
                            "unit": "kWh"
                        },
                        "rate": {
                            "value": number,
                            "unit": "€/kWh"
                        },
                        "amount": number
                    }
                },
                "totalAmount": {
                    "value": number,
                    "unit": "€"
                },
                "supplierContact": {
                    "address": "string",
                    "phone": ["string"],
                    "email": "string",
                    "website": "string",
                    "vatNumber": "string"
                }
            }
            ```
            '''
        else:  # gas
            return '''
            GAS BILL JSON SCHEMA (Required format):
            ```json
            {
                "documentType": "Natural Gas Bill",
                "supplierInfo": {
                    "name": "string",
                    "vatRegNo": "string",
                    "phoneNumber": "string",
                    "email": "string",
                    "address": {
                        "street": "string",
                        "city": "string", 
                        "postalCode": "string"
                    }
                },
                "customerInfo": {
                    "name": "string",
                    "address": {
                        "street": "string",
                        "city": "string",
                        "postalCode": "string"
                    },
                    "contactNumber": "string"
                },
                "accountInfo": {
                    "accountNumber": "string",
                    "gprn": "string",
                    "meterNumber": "string",
                    "tariffCategory": "string",
                    "paymentMethod": "string"
                },
                "billSummary": {
                    "invoiceNumber": "string",
                    "issueDate": "YYYY-MM-DD",
                    "dueDate": "YYYY-MM-DD",
                    "billingPeriodStartDate": "YYYY-MM-DD", 
                    "billingPeriodEndDate": "YYYY-MM-DD",
                    "lastBillAmount": number,
                    "paymentReceivedAmount": number,
                    "balanceBroughtForward": number,
                    "netBillAmount": number,
                    "totalVatAmount": number,
                    "currentBillAmount": number,
                    "totalDueAmount": number
                },
                "meterReadings": {
                    "previousReading": "string",
                    "presentReading": "string",
                    "unitsConsumed": number
                },
                "consumptionDetails": {
                    "consumptionValue": number,
                    "consumptionUnit": "kWh",
                    "calibrationValue": number,
                    "conversionFactor": number,
                    "correctionFactor": number
                },
                "lineItems": [
                    {
                        "description": "string",
                        "units": number,
                        "rate": number,
                        "vatCode": "string",
                        "amount": number
                    }
                ]
            }
            ```
            '''
    
    def _claude_extraction_shot(self, image_data: List[str], prompt: str) -> Dict:
        """Single Claude extraction shot"""
        
        message = self.client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=4000,
            messages=[
                {
                    "role": "user", 
                    "content": [
                        {"type": "text", "text": prompt},
                        *[{
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": img_data
                            }
                        } for img_data in image_data]
                    ]
                }
            ]
        )
        
        response_text = message.content[0].text
        return self._parse_claude_response(response_text)
    
    def _quick_validation_check(self, extraction: Dict, pdf_intelligence: Dict, bill_type: str) -> List[str]:
        """Quick validation to see if refinement shot is needed"""
        issues = []
        pdf_numbers = set(pdf_intelligence.get('all_numbers', []))
        
        # Check if key numbers are missing from PDF
        if bill_type == "electricity":
            consumption = extraction.get("consumption", [])
            for cons in consumption:
                value = cons.get("units", {}).get("value", 0)
                if value > 0 and value not in pdf_numbers:
                    issues.append(f"{cons.get('type')} consumption {value} not found in PDF")
        
        elif bill_type == "gas":
            consumption_value = extraction.get("consumptionDetails", {}).get("consumptionValue", 0)
            if consumption_value > 0 and consumption_value not in pdf_numbers:
                issues.append(f"Gas consumption {consumption_value} not found in PDF")
        
        # Check if total amount is reasonable
        if bill_type == "electricity":
            total = extraction.get("totalAmount", {}).get("value", 0)
        else:
            total = extraction.get("billSummary", {}).get("currentBillAmount", 0)
        
        if total <= 0 or total > 50000:  # Unreasonable total
            issues.append(f"Unreasonable total amount: {total}")
        
        return issues
    
    def _create_refinement_prompt(self, initial_extraction: Dict, issues: List[str], pdf_intelligence: Dict, bill_type: str) -> str:
        """Create refinement prompt to fix specific issues"""
        
        return f"""
        REFINEMENT REQUIRED: The initial extraction had these issues:
        {chr(10).join(f"- {issue}" for issue in issues)}
        
        Here was the initial extraction:
        ```json
        {json.dumps(initial_extraction, indent=2)}
        ```
        
        PDF numbers available for cross-reference:
        {pdf_intelligence.get('all_numbers', [])[:50]}
        
        Please re-examine the bill images and provide a corrected extraction.
        Focus specifically on the issues mentioned above.
        Use the exact same JSON schema as before.
        
        Pay extra attention to:
        1. Verifying all numbers exist in the PDF
        2. Double-checking decimal precision
        3. Ensuring mathematical consistency
        4. Cross-referencing with table data
        
        Provide only the corrected JSON extraction.
        """
    
    # Include all other methods (validation, parsing, etc.) from previous versions
    def _convert_images_to_base64(self, images: List) -> List[str]:
        """Convert PIL images to base64"""
        image_data = []
        for img in images:
            buffer = io.BytesIO()
            img.save(buffer, format='PNG')
            img_base64 = base64.b64encode(buffer.getvalue()).decode()
            image_data.append(img_base64)
        return image_data
    
    def _parse_claude_response(self, response_text: str) -> Dict:
        """Parse Claude JSON response"""
        try:
            json_match = re.search(r'```json\n(.*?)\n```', response_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            return json.loads(response_text)
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            raise e
    
    # ... (include all other helper methods from previous versions)


# MAIN INTEGRATION FUNCTION  
def process_with_smart_bill_extractor(content: bytes, bill_type: str, filename: str, api_key: str) -> Dict:
    """Professional bill extraction with Claude AI"""
    extractor = SmartBillExtractor(api_key)
    return extractor.extract_bill_data(content, bill_type, filename)
