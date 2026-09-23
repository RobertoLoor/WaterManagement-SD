@echo off
REM ============================================================
REM  Prepara un entorno virtual de Python SIN necesitar permisos
REM  de administrador (venv + pip --user se instalan en la carpeta
REM  del propio proyecto). Requiere que Python ya este instalado
REM  (o usar la version "embeddable" de python.org sin instalador).
REM ============================================================
cd /d "%~dp0\.."

echo Creando entorno virtual en .venv ...
python -m venv .venv

call .venv\Scripts\activate.bat

echo Instalando dependencias...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo.
echo Listo. Para usar el entorno en una nueva consola ejecuta:
echo   .venv\Scripts\activate.bat
pause
