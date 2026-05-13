@echo off
chcp 65001 >nul
cd /d "%~dp0.."
echo [%date% %time%] 启动调度器...

REM 激活虚拟环境（如果存在）
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM 启动调度器
python run_scheduler.py >> logs\scheduler_startup.log 2>&1
