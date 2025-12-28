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

set "INPUT_PATH=%REPO_ROOT%\data\samples\sample_messy.xlsx"
set "APP_PATH=%REPO_ROOT%\demos\tui_app.py"

"%PYTHON_EXE%" "%APP_PATH%" --input "%INPUT_PATH%" --interactive
