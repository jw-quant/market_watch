@echo off
cd /d C:\Users\senio\OneDrive\Desktop\repos\market_watch
echo [%date% %time%] Starting premarket.py >> logs\scheduler.log 2>&1
python jobs\premarket.py >> logs\scheduler.log 2>&1
echo [%date% %time%] premarket.py finished with exit code %errorlevel% >> logs\scheduler.log 2>&1
