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
├── docker-compose.yml         # Parte Cloud (solo pruebas en local): Kafka KRaft + WM_Central
├── docker-compose.lab.yml     # Parte Laboratorio: WS (Monitor+Engine) y FO
├── .env.lab.example           # plantilla de variables del laboratorio
├── requirements.txt
├── utils/                     # protocol.py, kafka_utils.py, db.py
├── WM_Central/                # lógica central + dashboard Flask
├── WM_WS/                     # WM_WS_M.py, WM_WS_E.py
├── WM_FO/                     # WM_FO.py
└── scripts/                   # lanzadores .bat para pruebas en Windows (sin Docker)
```

## Estado de las estaciones al arrancar (punto 1 del enunciado)

Al (re)iniciar, CENTRAL **borra todo lo que sabía del estado** de las estaciones
guardadas en la BD (estado, caudal, volumen y operario) y las muestra como
**DESCONECTADA** (gris). Se conservan el ID y la ubicación. Una estación solo
pasa a su estado real cuando su Monitor se (re)registra y el Engine está
conectado; si había una fuga, CENTRAL la recupera sola. Si un Monitor deja de dar
señales de vida (15 s) la estación vuelve a DESCONECTADA.

> Guía paso a paso para usarlo en local, en Railway y en varios ordenadores:
> **`GUIA_USO.md`**.

## 3. Despliegue en la nube (Railway) — obligatorio

Se despliega **Kafka + WM_Central + su BD** en Railway (Kafka funciona en modo
KRaft, sin Zookeeper). Railway **no usa** `docker-compose.yml`: cada servicio
se crea a mano dentro del mismo proyecto.

1. Crea un proyecto en Railway y conecta este repositorio de GitHub.
2. **Servicio `kafka`**: nuevo servicio desde la imagen Docker
   `bitnamilegacy/kafka:4.0.0-debian-12-r10`, con las variables `KAFKA_CFG_*`
   del `docker-compose.yml`. Activa un **TCP Proxy** para el puerto `9093`
   (listener `EXTERNAL`) y copia el dominio/puerto público que te asigna.
3. En el servicio `kafka` define `RAILWAY_RUN_UID=0` (los volúmenes de Railway
   se montan como administrador) y las variables `KAFKA_CFG_*` (lista completa
   en `GUIA_USO.md`, apartado B.3). Lo importante es el listener anunciado:
   `KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://${{RAILWAY_PRIVATE_DOMAIN}}:9092,EXTERNAL://${{RAILWAY_TCP_PROXY_DOMAIN}}:${{RAILWAY_TCP_PROXY_PORT}}`.
   Además:
   - `KAFKA_CFG_LOG_RETENTION_MINUTES=60`, `KAFKA_CFG_LOG_ROLL_MS=600000` y
     `KAFKA_CFG_LOG_RETENTION_CHECK_INTERVAL_MS=60000`: limitan el espacio que
     ocupa Kafka en el volumen (sin ellas crece sin parar).
4. **Servicio `wm_central`**: nuevo servicio desde este repositorio, con la
   variable `RAILWAY_DOCKERFILE_PATH=WM_Central/Dockerfile` (el contexto de
   build es la raíz del repo, porque el Dockerfile necesita `utils/`).
   Variables: `KAFKA_BROKER=${{kafka.RAILWAY_PRIVATE_DOMAIN}}:9092`,
   `SOCKET_PORT=5000`, `PORT=8080` y, para que la BD SQLite persista, un volumen
   montado en `/app/data` con `DB_PATH=/app/data/wm_central.db`.
5. **Puertos de `wm_central`:**
   - **Dashboard web:** Railway asigna la variable `PORT` y el dashboard
     escucha en `0.0.0.0:$PORT` (así lo hace `WM_Central.py`). Genera un
     dominio público para acceder desde el navegador:
     `https://<tu-wm-central>.up.railway.app` apuntando al puerto 8080 (el de
     `PORT`). No definas `PORT=5000`, para no chocar con los sockets.
   - **Sockets de los Monitores:** puerto `5000` (`SOCKET_PORT`), expuesto con
     un **TCP Proxy** hacia el puerto interno `5000`.
6. Anota los 3 datos que necesitarás en el laboratorio: **dominio y
   puerto público de WM_Central** (sockets, `CENTRAL_HOST` y `CENTRAL_PORT`) y
   **broker público de Kafka** (`KAFKA_BROKER`).

> **Nota Kafka+Railway:** el listener `EXTERNAL` de Kafka debe anunciar
> (`ADVERTISED_LISTENERS`) exactamente el host:puerto público que asigna el
> TCP Proxy, o los clientes externos no podrán completar la conexión aunque
> el `bootstrap` inicial funcione. Por eso se construye con las
> variables `RAILWAY_TCP_PROXY_DOMAIN/PORT` (en local, `docker-compose.yml` usa
> `KAFKA_EXTERNAL_HOST/PORT`).

### Probarlo en local antes de subirlo a Railway

```bash
docker compose up --build
```

Esto levanta Kafka y WM_Central en tu máquina
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

### 4.2. Alternativa sin Docker (pruebas y desarrollo)

El despliegue de la evaluación se hace con Docker (apartado 4.1), como exige el
enunciado. Esta alternativa sirve para **probar y desarrollar** sin Docker, y
como recurso si un PC concreto no permite instalarlo (en ese caso, consúltalo
antes con el profesor). Docker Desktop en Windows normalmente **sí requiere
permisos de administrador** (WSL2/Hyper-V); sin ellos se pueden ejecutar los
módulos directamente con Python, sin instalar nada fuera de la carpeta del
proyecto:

1. Si no hay Python instalado y no tienes permisos, descarga el
   **"Windows embeddable package"** desde python.org (es un .zip, no
   requiere instalador ni admin) y descomprímelo en cualquier carpeta.
2. Ejecuta `scripts\setup_windows.bat`: crea un entorno virtual `.venv`
   dentro del propio proyecto e instala `kafka-python-ng` y
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
