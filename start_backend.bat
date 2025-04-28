@echo off
cd /d "%~dp0"
echo Activating virtual environment...
call venv\Scripts\activate.bat
echo.
echo Running backend server...
uvicorn app.main:app --reload
pause
