@echo off
chcp 65001 >nul
echo [%date% %time%] 启动 HubStudio...

REM 检查是否已运行
tasklist /FI "IMAGENAME eq Hubstudio.exe" 2>NUL | find /I /N "Hubstudio.exe">NUL
if "%ERRORLEVEL%"=="0" (
    echo [%date% %time%] HubStudio 已在运行
    exit /b 0
)

REM 启动 HubStudio
start "" "D:\Program Files\Hubstudio\Hubstudio.exe"

REM 等待 API 就绪（最多等待 60 秒）
set /a count=0
:wait_loop
timeout /t 2 /nobreak >nul
curl -s http://127.0.0.1:6873 >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [%date% %time%] HubStudio API 已就绪
    exit /b 0
)
set /a count+=1
if %count% LSS 30 goto wait_loop

echo [%date% %time%] 警告: HubStudio API 未在 60 秒内就绪
exit /b 1
