@echo off
echo 正在移除开机自动启动...

schtasks /Delete /TN "AutoStart_HubStudio" /F
schtasks /Delete /TN "AutoStart_Scheduler" /F

echo.
echo ? 自动启动已移除
pause
