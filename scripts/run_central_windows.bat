@echo off
REM ============================================================
REM  Lanza WM_Central sin Docker (util en desarrollo/pruebas locales,
REM  cuando Kafka ya esta corriendo, por ejemplo via docker-compose.yml).
REM  Uso:
REM    run_central_windows.bat <puerto_sockets> <kafka_broker> [puerto_web]
REM  Ejemplo:
REM    run_central_windows.bat 5000 127.0.0.1:9093 8080
REM ============================================================
cd /d "%~dp0\.."
call .venv\Scripts\activate.bat

python WM_Central\WM_Central.py %1 %2 %3
