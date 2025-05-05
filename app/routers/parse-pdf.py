@router.post("/utilities/parse-pdf")
async def parse_pdf(file: UploadFile = File(...)):
    try:
        content = await file.read()
        parsed = parse_arden(content)
        return {"full_data": parsed}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": f"Parse failed: {str(e)}"}
        )
