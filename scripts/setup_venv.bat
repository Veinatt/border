@echo off
setlocal

cd /d "%~dp0.."

py -3.10 -m venv .venv
call ".venv\Scripts\activate.bat"
python -m pip install --upgrade pip
pip install -r requirements.txt

echo.
echo Environment setup complete.
python --version
pip --version

endlocal
