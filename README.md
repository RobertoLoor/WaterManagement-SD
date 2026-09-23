# WaterManagement — Sistemas Distribuidos 26/27

Simulación distribuida de una red de riego urbano: **CENTRAL**, **estaciones de
riego (WS = Monitor + Engine)** y **operarios de campo (FO)**, comunicados
mediante **sockets** (protocolo `<STX><DATA><ETX><LRC>`) y **Apache Kafka**
(streaming de eventos), con persistencia en **SQLite** y un panel de
monitorización web en tiempo real.

## 1. Componentes

| Componente     | Fichero                       | Rol |
|----------------|--------------------------------|-----|
| **WM_Central** | `WM_Central/WM_Central.py`    | Lógica central: alta de WS, autorización de riegos, BD, dashboard web |
| **Monitor**    | `WM_WS/WM_WS_M.py`            | Registra la WS en CENTRAL y vigila la salud del Engine (fugas) |
| **Engine**     | `WM_WS/WM_WS_E.py`            | Simula el caudalímetro/electroválvula; ejecuta el riego |
| **WM_FO**      | `WM_FO/WM_FO.py`              | App del operario: consulta WS y solicita riegos |

Comunicación:

- **Sockets** (`utils/protocol.py`): `WM_WS_M` ↔ `WM_Central` (registro) y
  `WM_WS_E` ↔ `WM_WS_M` (latido de salud cada segundo, dentro de la misma WS).
- **Kafka** (`utils/kafka_utils.py`): todo lo demás (telemetría, comandos,
  peticiones de riego, respuestas y el "broadcast" del estado global).

Topics Kafka usados: `telemetria-riego`, `comandos-estacion`, `estado-ws`,
`riego-peticiones`, `riego-respuestas`, `central-broadcast`.

## 2. Estructura del proyecto

```
WaterManagement/
├── docker-compose.yml         # Parte Cloud: Zookeeper + Kafka + WM_Central
├── docker-compose.lab.yml     # Parte Laboratorio: WS (Monitor+Engine) y FO
├── .env / .env.lab.example
├── requirements.txt
├── utils/                     # protocol.py, kafka_utils.py, db.py
├── WM_Central/                # lógica central + dashboard Flask
├── WM_WS/                     # WM_WS_M.py, WM_WS_E.py
├── WM_FO/                     # WM_FO.py
└── scripts/                   # lanzadores .bat para Windows SIN Docker
```

## 3. Despliegue en la nube (Railway) — obligatorio

Se despliega **Zookeeper + Kafka + WM_Central + su BD** en Railway.

1. Crea un proyecto en Railway y conecta este repositorio de GitHub.
2. Railway detectará `docker-compose.yml` (o crea 3 servicios manualmente:
   `zookeeper`, `kafka`, `wm_central`, apuntando cada uno a su Dockerfile).
3. En el servicio `kafka`, activa un **TCP Proxy** para el puerto `9093`
   (listener `EXTERNAL`) y copia el dominio/puerto público que te asigna.
4. Define en Railway las variables de entorno:
   - `KAFKA_EXTERNAL_HOST` = dominio público del TCP Proxy de Kafka
   - `KAFKA_EXTERNAL_PORT` = puerto público del TCP Proxy de Kafka
5. En el servicio `wm_central`, expón también su puerto de sockets (`5000`,
   variable `SOCKET_PORT`) con un TCP Proxy, y su puerto web (`8080`,
   variable `WEB_PORT`) como dominio HTTP público — así accedes al
   dashboard desde el navegador: `https://<tu-wm-central>.up.railway.app`.
6. Anota los 3 datos que necesitarás en el laboratorio: **IP/dominio y
   puerto público de WM_Central** (sockets) y **broker público de Kafka**.

> **Nota Kafka+Railway:** el listener `EXTERNAL` de Kafka debe anunciar
> (`ADVERTISED_LISTENERS`) exactamente el host:puerto público que asigna el
> TCP Proxy, o los clientes externos no podrán completar la conexión aunque
> el `bootstrap` inicial funcione. Por eso `KAFKA_EXTERNAL_HOST/PORT` son
> variables de entorno explícitas en `docker-compose.yml`.

### Probarlo en local antes de subirlo a Railway

```bash
docker compose up --build
```

Esto levanta Zookeeper, Kafka y WM_Central en tu máquina
(`KAFKA_EXTERNAL_HOST=127.0.0.1`, `KAFKA_EXTERNAL_PORT=9093` por defecto).
Dashboard: http://localhost:8080

## 4. Laboratorio — WS y FO

Solo se despliegan aquí los módulos **WM_WS** (Monitor+Engine) y **WM_FO**,
apuntando siempre a la parte Cloud ya desplegada en Railway.

### 4.1. Con Docker (si el PC tiene permisos de administrador / Docker instalado)

```bash
cp .env.lab.example .env.lab      # y rellena CENTRAL_HOST, CENTRAL_PORT, KAFKA_BROKER
docker compose -f docker-compose.lab.yml --env-file .env.lab up --build -d ws01_monitor ws01_engine
docker compose -f docker-compose.lab.yml --env-file .env.lab run --rm fo1
```

(`ws01_engine` y `fo1` necesitan consola interactiva para su menú/teclado;
por eso se recomienda `run --rm` en vez de `up -d` para esos dos.)

### 4.2. Sin permisos de administrador (PCs restringidos del laboratorio)

Docker Desktop en Windows normalmente **sí requiere permisos de
administrador** para instalarse (WSL2/Hyper-V), así que en un PC sin esos
permisos la alternativa es ejecutar los módulos directamente con Python,
sin instalar nada fuera de la carpeta del proyecto:

1. Si no hay Python instalado y no tienes permisos, descarga el
   **"Windows embeddable package"** desde python.org (es un .zip, no
   requiere instalador ni admin) y descomprímelo en cualquier carpeta.
2. Ejecuta `scripts\setup_windows.bat`: crea un entorno virtual `.venv`
   dentro del propio proyecto e instala `kafka-python`, `python-dotenv` y
   `Flask` con `pip install --user`-equivalente (todo dentro de la carpeta,
   sin tocar el sistema).
3. Lanza una WS completa (Monitor + Engine, cada uno en su consola):
   ```
   scripts\run_ws_windows.bat central.up.railway.app 5000 WS-01 "River Park" 6001 broker.up.railway.app:9093
   ```
4. Lanza el operario:
   ```
   scripts\run_fo_windows.bat broker.up.railway.app:9093 FO-01
   ```

Esta vía cumple igualmente la arquitectura (sockets + Kafka) y es la que se
recomienda para portátiles/aulas sin privilegios de instalación; los
Dockerfiles de `WM_WS/` y `WM_FO/` quedan disponibles para cuando sí se
disponga de Docker.

## 5. Simular incidencias y probar el flujo completo

1. Arranca CENTRAL (Railway) y comprueba el dashboard.
2. Arranca una WS (Monitor + Engine) → aparece en el dashboard en verde
   (`DISPONIBLE`).
3. Arranca `WM_FO`, opción **1** para ver las estaciones, opción **2** para
   solicitar un riego (verás caudal/volumen en tiempo real en el dashboard,
   parpadeando en verde).
4. En la consola del **Engine**, pulsa `k` + Enter para simular una fuga:
   la WS pasa a rojo (`FUGA`) tanto en CENTRAL como en el mensaje al
   operario; `o` + Enter la resuelve.
5. Desde el propio dashboard de CENTRAL puedes **Bloquear**/**Activar**
   cualquier WS o forzar un riego manual (punto 11 del enunciado).
6. Opción **3** de `WM_FO` para lanzar `activaciones_ejemplo.json` y probar
   el modo automático (una petición cada 4s tras concluir la anterior).

## 6. Sobre el uso de IA

Los prompts usados con asistentes de IA durante el desarrollo se incluyen
en el repositorio de GitHub, tal y como exige el enunciado.
