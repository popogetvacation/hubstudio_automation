@echo off
echo 正在配置开机自动启动...

set SCRIPT_DIR=%~dp0

schtasks /Create /TN "AutoStart_HubStudio" /TR ""%SCRIPT_DIR%start_hubstudio.bat"" /SC ONLOGON /DELAY 0000:10 /RL HIGHEST /F
if %ERRORLEVEL% NEQ 0 (
    echo 创建 HubStudio 任务失败，请以管理员身份运行
    pause
    exit /b 1
)

schtasks /Create /TN "AutoStart_Scheduler" /TR ""%SCRIPT_DIR%start_scheduler.bat"" /SC ONLOGON /DELAY 0001:30 /RL HIGHEST /F
if %ERRORLEVEL% NEQ 0 (
    echo 创建 Scheduler 任务失败
    pause
    exit /b 1
)

echo.
echo 自动启动配置完成！
echo.
echo 已创建以下任务：
echo   - AutoStart_HubStudio (登录后 10 秒启动)
echo   - AutoStart_Scheduler (登录后 90 秒启动)
echo.
echo 可以通过"任务计划程序"查看和管理这些任务
pause
