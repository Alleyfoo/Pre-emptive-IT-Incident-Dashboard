@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"

set "PYTHON_EXE="
for /f "delims=" %%P in ('where python 2^>nul') do (
  set "PYTHON_EXE=%%P"
  goto :python_found
)
:python_found
if "%PYTHON_EXE%"=="" set "PYTHON_EXE=python"

set "APP_PATH=%REPO_ROOT%\demos\dashboard.py"

"%PYTHON_EXE%" -m streamlit run "%APP_PATH%"
