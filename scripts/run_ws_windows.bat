@echo off
REM ============================================================
REM  Lanza el Monitor y el Engine de una estacion de riego (WS)
REM  sin Docker, usando el entorno virtual .venv (ver setup_windows.bat).
REM  Uso:
REM    run_ws_windows.bat <ip_central> <puerto_central> <id_ws> <ubicacion> <puerto_engine> <kafka_broker>
REM  Ejemplo:
REM    run_ws_windows.bat central.up.railway.app 5000 WS-01 "River Park" 6001 broker.up.railway.app:9093
REM ============================================================
cd /d "%~dp0\.."
call .venv\Scripts\activate.bat

set IP_CENTRAL=%1
set PUERTO_CENTRAL=%2
set ID_WS=%3
set UBICACION=%4
set PUERTO_ENGINE=%5
set KAFKA_BROKER=%6

start "Monitor %ID_WS%" cmd /k python WM_WS\WM_WS_M.py %IP_CENTRAL% %PUERTO_CENTRAL% %ID_WS% %UBICACION% %PUERTO_ENGINE% %KAFKA_BROKER%
timeout /t 2 >nul
start "Engine %ID_WS%" cmd /k python WM_WS\WM_WS_E.py 127.0.0.1 %PUERTO_ENGINE% %KAFKA_BROKER%
