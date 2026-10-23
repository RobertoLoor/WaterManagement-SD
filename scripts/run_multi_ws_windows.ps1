# scripts/run_multi_ws_windows.ps1
# ============================================================
# Levanta 20 estaciones de riego (WS-01 a WS-20).
# Cada estacion abre 2 ventanas CMD: Monitor y Engine.
#
# Uso (desde la raiz del proyecto o desde scripts/):
#   .\scripts\run_multi_ws_windows.ps1
#   .\scripts\run_multi_ws_windows.ps1 -IpCentral 127.0.0.1 -PuertoCentral 5000 -KafkaBroker 127.0.0.1:9093
#
# Ejemplo Railway:
#   .\scripts\run_multi_ws_windows.ps1 -IpCentral nozomi.proxy.rlwy.net -PuertoCentral 31147 -KafkaBroker algo.proxy.rlwy.net:12345
#
# Puertos de Engine: 6001 (WS-01) ... 6020 (WS-20)
# ============================================================

param(
    [string]$IpCentral    = "127.0.0.1",
    [int]   $PuertoCentral = 5000,
    [string]$KafkaBroker  = "127.0.0.1:9093"
)

# Ir a la raiz del proyecto (un nivel por encima de scripts/)
$raiz = Split-Path $PSScriptRoot -Parent
Set-Location $raiz

# Ruta al interprete Python del entorno virtual
$python = Join-Path $raiz ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    Write-Host "[ERROR] No se encontro el entorno virtual en .venv\Scripts\python.exe" -ForegroundColor Red
    Write-Host "        Ejecuta primero: scripts\setup_windows.bat" -ForegroundColor Yellow
    exit 1
}

# Ubicaciones de las 20 estaciones
$ubicaciones = @(
    "Parque Norte",
    "Parque Sur",
    "Jardin Central",
    "Plaza Mayor",
    "Avenida Principal",
    "Barrio Este",
    "Barrio Oeste",
    "Zona Deportiva",
    "Parque Infantil",
    "Rotonda Sur",
    "Bulevar Norte",
    "Glorieta Este",
    "Campus Universidad",
    "Zona Industrial",
    "Parque Tecnologico",
    "Ribera del Rio",
    "Monte Pinar",
    "Huerta Municipal",
    "Poligono Sur",
    "Alameda Central"
)

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Levantando 20 estaciones de riego (WS-01 a WS-20)" -ForegroundColor Cyan
Write-Host "  Central : ${IpCentral}:${PuertoCentral}" -ForegroundColor Cyan
Write-Host "  Kafka   : $KafkaBroker" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

for ($i = 1; $i -le 20; $i++) {
    $id        = "WS-{0:D2}" -f $i          # WS-01, WS-02 ... WS-20
    $puerto    = 6000 + $i                   # 6001 ... 6020
    $ubicacion = $ubicaciones[$i - 1]

    Write-Host "  Arrancando $id  |  $ubicacion  |  puerto Engine: $puerto" -ForegroundColor Green

    # --- Monitor ---
    $argsMonitor = "/k title Monitor $id && `"$python`" WM_WS\WM_WS_M.py $IpCentral $PuertoCentral $id `"$ubicacion`" $puerto $KafkaBroker"
    Start-Process cmd -ArgumentList $argsMonitor

    # Pausa para que el Monitor se registre antes de que el Engine conecte
    Start-Sleep -Milliseconds 1500

    # --- Engine ---
    $argsEngine = "/k title Engine $id && `"$python`" WM_WS\WM_WS_E.py 127.0.0.1 $puerto $KafkaBroker"
    Start-Process cmd -ArgumentList $argsEngine

    # Pequeña pausa entre estaciones para no saturar Central de golpe
    Start-Sleep -Milliseconds 300
}

Write-Host ""
Write-Host "  20 estaciones lanzadas (40 ventanas abiertas)." -ForegroundColor Green
Write-Host "  Abre http://localhost:8080 para ver el panel web." -ForegroundColor Green
Write-Host ""
