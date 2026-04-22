@echo off
setlocal

REM Run scraper scheduler from project root.
cd /d "%~dp0.."

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found: .venv
    echo Create it first: py -3.10 -m venv .venv
    pause
    exit /b 1
)

call ".venv\Scripts\activate.bat"
python main_scraper.py

endlocal
