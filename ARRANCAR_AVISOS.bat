@echo off
setlocal
cd /d "%~dp0"
set "STREAMLIT_EXE=C:\Users\jaime\AppData\Roaming\Python\Python312\Scripts\streamlit.exe"

if not exist "%STREAMLIT_EXE%" (
  echo No encuentro Streamlit en:
  echo %STREAMLIT_EXE%
  pause
  exit /b 1
)

start "" http://localhost:8501
"%STREAMLIT_EXE%" run app.py
