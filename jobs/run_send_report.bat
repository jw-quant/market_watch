@echo off
cd /d C:\Users\senio\OneDrive\Desktop\repos\market_watch
echo [%date% %time%] Starting send_report.py >> logs\scheduler.log 2>&1
python jobs\send_report.py >> logs\scheduler.log 2>&1
echo [%date% %time%] send_report.py finished with exit code %errorlevel% >> logs\scheduler.log 2>&1
