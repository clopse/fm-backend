def process_and_store_docupanda(db, content, hotel_id, utility_type, supplier, filename):
    try:
        encoded = base64.b64encode(content).decode()

        # Step 1: Upload document
        upload_res = requests.post(
            "https://app.docupanda.io/document",
            json={"document": {"file": {"contents": encoded, "filename": filename}}},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )

        print(f"📤 Upload response: {upload_res.status_code} - {upload_res.text}")
        data = upload_res.json()
        document_id = data.get("documentId")
        job_id = data.get("jobId")
        if not document_id or not job_id:
            print("❌ Missing documentId or jobId.")
            return

        # Step 2: Poll job completion
        for attempt in range(10):
            time.sleep(6)
            res = requests.get(
                f"https://app.docupanda.io/job/{job_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            print(f"🕓 Upload job status: {res.get('status')}")
            if res.get("status") == "completed":
                break
            if res.get("status") == "error":
                print("❌ Upload job failed.")
                return
        else:
            print("❌ Upload job timeout.")
            return

        # Step 3: Wait for document to be ready
        for attempt in range(10):
            time.sleep(10)
            doc_check = requests.get(
                f"https://app.docupanda.io/document/{document_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            doc_status = doc_check.get("status")
            print(f"📄 Document status: {doc_status}")
            if doc_status == "ready":
                break
        else:
            print("❌ Document never reached 'ready' status.")
            return

        # Step 4: Parse to detect bill type
        pages_text = doc_check.get("result", {}).get("pagesText", [])
        detected_type = detect_bill_type(pages_text)
        schema_id = SCHEMA_ELECTRICITY if detected_type == "electricity" else SCHEMA_GAS

        # Step 5: Standardize
        std_res = requests.post(
            "https://app.docupanda.io/standardize/batch",
            json={"documentIds": [document_id], "schemaId": schema_id},
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "X-API-Key": DOCUPANDA_API_KEY,
            },
        )

        print(f"⚙️ Standardization response: {std_res.status_code} - {std_res.text}")
        std_data = std_res.json()
        std_id = std_data.get("standardizationId")
        if not std_id:
            print("❌ No standardizationId returned.")
            return

        # Step 6: Poll for standardization
        for attempt in range(10):
            time.sleep(6)
            result = requests.get(
                f"https://app.docupanda.io/standardize/{std_id}",
                headers={"accept": "application/json", "X-API-Key": DOCUPANDA_API_KEY},
            ).json()
            if result.get("status") == "completed":
                parsed = result.get("result", {})
                billing_start = parsed.get("billingPeriod", {}).get("startDate") or datetime.utcnow().strftime("%Y-%m-%d")
                s3_path = save_json_to_s3(parsed, hotel_id, detected_type, billing_start, filename)
                save_parsed_data_to_db(db, hotel_id, detected_type, parsed, s3_path)
                print(f"✅ Bill parsed and saved: {s3_path}")
                return
            elif result.get("status") == "error":
                print(f"❌ Standardization error: {result}")
                return
        else:
            print("❌ Standardization polling timed out.")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
