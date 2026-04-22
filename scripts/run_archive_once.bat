@echo off
setlocal

REM Run archive scraper once to backfill historical data.
cd /d "%~dp0.."

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found: .venv
    echo Create it first: py -3.10 -m venv .venv
    pause
    exit /b 1
)

call ".venv\Scripts\activate.bat"
python scrapers/archive_scraper.py

endlocal
