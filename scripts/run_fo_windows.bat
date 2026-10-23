@echo off
REM ============================================================
REM  Lanza WM_FO (aplicacion del operario) sin Docker.
REM  Uso:
REM    run_fo_windows.bat <kafka_broker> <id_operador> [fichero_activaciones]
REM  Ejemplo:
REM    run_fo_windows.bat broker.up.railway.app:9093 FO-01
REM ============================================================
cd /d "%~dp0\.."
call .venv\Scripts\activate.bat

python WM_FO\WM_FO.py %1 %2 %3
