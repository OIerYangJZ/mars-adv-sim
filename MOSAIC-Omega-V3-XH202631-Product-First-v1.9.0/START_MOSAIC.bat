@echo off
setlocal EnableExtensions
cd /d "%~dp0"
title MOSAIC-Omega v1.9.0

echo ===============================================
echo  MOSAIC-Omega v1.9.0 - Windows Launcher
echo ===============================================
echo.

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

rem 1. Prefer local virtual environment if provided
if exist ".venv\Scripts\python.exe" (
    echo [MOSAIC] Using project virtual environment
    ".venv\Scripts\python.exe" scripts\windows_launcher.py
    goto end
)

rem 2. Use installed Python
where python.exe >nul 2>nul
if not errorlevel 1 (
    echo [MOSAIC] Using system Python
    python.exe scripts\windows_launcher.py
    goto end
)

rem 3. Try Python launcher
where py.exe >nul 2>nul
if not errorlevel 1 (
    echo [MOSAIC] Using Python Launcher
    py -3.11 scripts\windows_launcher.py
    goto end
)

echo.
echo [MOSAIC] Python not found.
echo Please install Python 3.11 x64 and run:
echo python -m pip install -r requirements.txt
set RC=20
goto fail

:end
set RC=%ERRORLEVEL%
if "%RC%"=="0" goto success

:fail
echo.
echo [MOSAIC] Startup failed. Exit code: %RC%
echo Check .mosaic_logs for details.
pause
exit /b %RC%

:success
echo.
echo [MOSAIC] Started successfully.
exit /b 0
