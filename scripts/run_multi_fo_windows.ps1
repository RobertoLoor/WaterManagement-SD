# scripts/run_multi_fo_windows.ps1
# ============================================================
# Levanta 15 operarios (FO-01 a FO-15) en MODO AUTOMATICO.
# Cada uno abre una ventana CMD y ejecuta su fichero JSON de
# activaciones sin interaccion del usuario.
#
# Uso (desde la raiz del proyecto o desde scripts/):
#   .\scripts\run_multi_fo_windows.ps1
#   .\scripts\run_multi_fo_windows.ps1 -KafkaBroker 127.0.0.1:9093
#
# Ejemplo Railway:
#   .\scripts\run_multi_fo_windows.ps1 -KafkaBroker algo.proxy.rlwy.net:12345
#
# Los ficheros JSON de activaciones estan en WM_FO\activaciones\FO-01.json ... FO-15.json
# ============================================================

param(
    [string]$KafkaBroker     = "127.0.0.1:9093",
    [string]$DirActivaciones = "WM_FO\activaciones"
)

# Ir a la raiz del proyecto
$raiz = Split-Path $PSScriptRoot -Parent
Set-Location $raiz

$python = Join-Path $raiz ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    Write-Host "[ERROR] No se encontro el entorno virtual en .venv\Scripts\python.exe" -ForegroundColor Red
    Write-Host "        Ejecuta primero: scripts\setup_windows.bat" -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path $DirActivaciones)) {
    Write-Host "[ERROR] No se encontro la carpeta de activaciones: $DirActivaciones" -ForegroundColor Red
    Write-Host "        Asegurate de que existe WM_FO\activaciones\ con los ficheros JSON." -ForegroundColor Yellow
    exit 1
}

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "  Levantando 15 operarios en modo automatico (FO-01..15)" -ForegroundColor Cyan
Write-Host "  Kafka : $KafkaBroker" -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

for ($i = 1; $i -le 15; $i++) {
    $id      = "FO-{0:D2}" -f $i            # FO-01 ... FO-15
    $fichero = "$DirActivaciones\$id.json"

    if (-not (Test-Path $fichero)) {
        Write-Host "  [WARN] No se encontro $fichero. Saltando $id." -ForegroundColor Yellow
        continue
    }

    Write-Host "  Arrancando $id  |  $fichero" -ForegroundColor Green

    # AUTO_MODE=1 activa el modo automatico en WM_FO.py (sin menu interactivo)
    $args = "/k title Operario $id && set AUTO_MODE=1 && `"$python`" WM_FO\WM_FO.py $KafkaBroker $id `"$fichero`""
    Start-Process cmd -ArgumentList $args

    # Pausa escalonada para no saturar Kafka con 15 conexiones simultaneas
    Start-Sleep -Milliseconds 400
}

Write-Host ""
Write-Host "  15 operarios lanzados. Cada uno ejecutara su fichero JSON automaticamente." -ForegroundColor Green
Write-Host ""
