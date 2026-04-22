@echo off
setlocal

REM Run Telegram bot from project root.
cd /d "%~dp0.."

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found: .venv
    echo Create it first: py -3.10 -m venv .venv
    pause
    exit /b 1
)

if "%TELEGRAM_BOT_TOKEN%"=="" (
    echo [ERROR] TELEGRAM_BOT_TOKEN is not set.
    echo Example: set TELEGRAM_BOT_TOKEN=123456:ABCDEF
    pause
    exit /b 1
)

call ".venv\Scripts\activate.bat"
python bot/main.py

endlocal
