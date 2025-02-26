#!/bin/bash
echo "Starting DBT Model Generator..."
python app_launcher.py
if [ $? -ne 0 ]; then
    echo "An error occurred while running the application."
    echo "Please check the launcher.log file for details."
    read -p "Press Enter to continue..."
fi 