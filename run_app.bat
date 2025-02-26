@echo off
echo Starting DBT Model Generator...
python app_launcher.py
if %ERRORLEVEL% NEQ 0 (
    echo An error occurred while running the application.
    echo Please check the launcher.log file for details.
    pause
) 