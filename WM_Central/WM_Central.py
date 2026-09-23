# WM_Central/WM_Central.py
"""
WM_Central - Nucleo del sistema WaterManagement.

Responsabilidades:
 - Servidor de sockets (protocolo STX/ETX/LRC) para el registro/autenticacion
   de los Monitores de las estaciones de riego (WM_WS_M).
 - Productor/consumidores Kafka para:
     * recibir telemetria de riego de los Engines (WM_WS_E)
     * recibir incidencias (fugas/desconexiones) de los Monitores
     * recibir peticiones de activacion de los operarios (WM_FO)
     * enviar comandos a los Engines (iniciar/detener/bloquear/activar)
     * enviar respuestas a los operarios (autorizado/denegado/resumen)
     * emitir un "broadcast" periodico con el estado global de la red
 - Persistencia en SQLite (utils/db.py).
 - Panel de monitorizacion web (Flask) en tiempo real (polling 1s).

Uso:
    python WM_Central.py <puerto_sockets> <kafka_broker> [puerto_web]

Variables de entorno equivalentes (usadas por defecto si no hay argumentos,
utiles para Docker): SOCKET_PORT, KAFKA_BROKER, WEB_PORT, DB_PATH
"""
import os
import sys
import socket
import threading
import time
import uuid

# --- localizar la raiz del proyecto (utils/ + .env) ------------------------
# --- localizar la raiz del proyecto (carpeta que contiene utils/) ---------
_inicio = os.path.dirname(os.path.abspath(__file__))
dir_cursor = _inicio
while True:
    if os.path.isdir(os.path.join(dir_cursor, "utils")):
        if dir_cursor not in sys.path:
            sys.path.insert(0, dir_cursor)
        break
    parent = os.path.dirname(dir_cursor)
    if parent == dir_cursor:
        raise RuntimeError(
            f"No se encontro utils/. Inicio={_inicio} | "
            f"contenido de {os.path.dirname(_inicio)}: "
            f"{sorted(os.listdir(os.path.dirname(_inicio)))}"
        )
    dir_cursor = parent

from utils.protocol import (
    empaquetar_trama, desempaquetar_trama, recv_exact_frame,
    ENQ, ACK, NACK, EOT,
)
from utils.kafka_utils import (
    crear_productor, crear_consumidor,
    TOPIC_TELEMETRIA, TOPIC_COMANDOS, TOPIC_ESTADO_WS,
    TOPIC_PETICIONES, TOPIC_RESPUESTAS, TOPIC_BROADCAST,
)
from utils.db import Database

# ---------------------------------------------------------------- estado ---
ESTADOS_QUE_PERMITEN_RIEGO = {"DISPONIBLE"}
riegos_activos = {}   # id_ws -> {request_id, id_operador, duracion_seg}
lock_riegos = threading.Lock()

db: Database = None
productor = None


# ============================================================ SOCKETS =====
def atender_monitor(conn, addr):
    """Atiende la conexion de un Monitor (WM_WS_M): ENQ -> ACK, registro -> alta en BD."""
    print(f"[SOCKET] Nueva conexion de Monitor desde {addr}")
    try:
        while True:
            datos = recv_exact_frame(conn, timeout=120)
            if not datos:
                print(f"[SOCKET] Monitor {addr} desconectado.")
                break

            if datos == ENQ:
                conn.sendall(ACK)
                continue
            if datos == EOT:
                break

            es_valido, mensaje = desempaquetar_trama(datos)
            if not es_valido:
                print(f"[SOCKET] Trama corrupta de {addr}")
                conn.sendall(NACK)
                continue

            conn.sendall(ACK)
            print(f"[SOCKET] Mensaje de {addr}: {mensaje}")

            partes = mensaje.split('#')
            comando = partes[0]

            if comando == "REGISTRO":
                id_ws = partes[1] if len(partes) > 1 else "WS-DESCONOCIDA"
                ubicacion = partes[2] if len(partes) > 2 else "DESCONOCIDA"
                db.upsert_estacion_registro(id_ws, ubicacion)
                db.log_evento(f"{id_ws} registrada/conectada en '{ubicacion}'.")
                respuesta = empaquetar_trama(f"REGISTRO_OK#{id_ws}")
                conn.sendall(respuesta)
    except (ConnectionResetError, socket.timeout) as e:
        print(f"[SOCKET] Conexion {addr} interrumpida: {e}")
    except Exception as e:
        print(f"[SOCKET][ERROR] {addr}: {e}")
    finally:
        conn.close()


def servidor_sockets(puerto: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', puerto))
    s.listen()
    print(f"[WM_CENTRAL] Servidor de sockets escuchando en el puerto {puerto}...")
    while True:
        conn, addr = s.accept()
        threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True).start()


# ============================================================== KAFKA =====
def enviar_comando(id_ws: str, comando: str, duracion_seg: int = None, operario: str = None):
    productor.send(TOPIC_COMANDOS, key=id_ws, value={
        "id_ws": id_ws, "comando": comando,
        "duracion_seg": duracion_seg, "operario": operario,
        "timestamp": time.time(),
    })
    productor.flush()


def responder_operario(id_operador: str, payload: dict):
    payload["id_operador"] = id_operador
    payload["timestamp"] = time.time()
    productor.send(TOPIC_RESPUESTAS, key=id_operador, value=payload)
    productor.flush()


def hilo_peticiones(broker: str):
    """Escucha las peticiones de riego que envian los operarios (WM_FO)."""
    consumidor = crear_consumidor(TOPIC_PETICIONES, broker, group_id="central-peticiones")
    print(f"[KAFKA] Escuchando peticiones en '{TOPIC_PETICIONES}'...")
    while True:
        for msg in consumidor:
            data = msg.value
            if data.get("tipo") != "SOLICITUD_RIEGO":
                continue
            id_ws = data.get("id_ws")
            id_operador = data.get("id_operador")
            duracion = data.get("duracion_seg", 60)
            request_id = data.get("request_id", str(uuid.uuid4()))

            estacion = db.get_estacion(id_ws)
            with lock_riegos:
                ya_regando = id_ws in riegos_activos

            if not estacion:
                responder_operario(id_operador, {
                    "request_id": request_id, "id_ws": id_ws, "evento": "DENEGADO",
                    "motivo": "La estacion no existe o nunca se ha conectado a CENTRAL.",
                })
                continue

            if ya_regando or estacion["estado"] not in ESTADOS_QUE_PERMITEN_RIEGO:
                motivos = {
                    "REGANDO": "La estacion ya esta regando en este momento.",
                    "FUGA": "Fuga detectada: el riego esta bloqueado hasta resolver la averia.",
                    "FUERA_DE_SERVICIO": "La estacion esta fuera de servicio por orden de CENTRAL.",
                    "DESCONECTADA": "La estacion no esta conectada a CENTRAL.",
                }
                motivo = motivos.get(estacion["estado"], "La estacion no esta disponible.")
                responder_operario(id_operador, {
                    "request_id": request_id, "id_ws": id_ws, "evento": "DENEGADO", "motivo": motivo,
                })
                db.log_evento(f"Riego DENEGADO en {id_ws} solicitado por {id_operador}: {motivo}")
                continue

            # Todo correcto: autorizar
            with lock_riegos:
                riegos_activos[id_ws] = {
                    "request_id": request_id, "id_operador": id_operador, "duracion_seg": duracion,
                }
            db.set_estado(id_ws, "REGANDO", operario=id_operador)
            enviar_comando(id_ws, "INICIAR_RIEGO", duracion_seg=duracion, operario=id_operador)
            responder_operario(id_operador, {
                "request_id": request_id, "id_ws": id_ws, "evento": "AUTORIZADO",
                "motivo": "Riego autorizado e iniciado.", "duracion_seg": duracion,
            })
            db.log_evento(f"Riego AUTORIZADO en {id_ws} por {id_operador} durante {duracion}s.")
        time.sleep(0.2)


def hilo_telemetria(broker: str):
    """Escucha la telemetria y los resumenes finales que envian los Engines (WM_WS_E)."""
    consumidor = crear_consumidor(TOPIC_TELEMETRIA, broker, group_id="central-telemetria")
    print(f"[KAFKA] Escuchando telemetria en '{TOPIC_TELEMETRIA}'...")
    while True:
        for msg in consumidor:
            data = msg.value
            id_ws = data.get("id_ws")
            tipo = data.get("tipo", "TELEMETRIA")

            if tipo == "TELEMETRIA":
                db.set_telemetria(id_ws, data.get("caudal_lmin", 0), data.get("volumen_total_l", 0))

            elif tipo == "RIEGO_FINALIZADO":
                db.finalizar_riego(id_ws)
                with lock_riegos:
                    info = riegos_activos.pop(id_ws, None)
                motivo = data.get("motivo", "MANUAL")
                db.log_evento(f"Riego finalizado en {id_ws} ({motivo}). "
                               f"Volumen total: {data.get('volumen_total_l', 0)} L.")
                if info:
                    responder_operario(info["id_operador"], {
                        "request_id": info["request_id"], "id_ws": id_ws, "evento": "RESUMEN",
                        "motivo": motivo, "volumen_total_l": data.get("volumen_total_l", 0),
                        "duracion_seg": data.get("duracion_seg", 0),
                    })
        time.sleep(0.2)


def hilo_estado_ws(broker: str):
    """Escucha eventos de salud (fuga/desconexion) enviados por los Monitores (WM_WS_M)."""
    consumidor = crear_consumidor(TOPIC_ESTADO_WS, broker, group_id="central-estado-ws")
    print(f"[KAFKA] Escuchando estado de WS en '{TOPIC_ESTADO_WS}'...")
    while True:
        for msg in consumidor:
            data = msg.value
            id_ws = data.get("id_ws")
            evento = data.get("evento")

            with lock_riegos:
                info_riego = riegos_activos.get(id_ws)

            if evento == "FUGA":
                db.set_estado(id_ws, "FUGA")
                db.log_evento(f"FUGA detectada en {id_ws}. Riego bloqueado.")
                if info_riego:
                    enviar_comando(id_ws, "DETENER_RIEGO")
                    responder_operario(info_riego["id_operador"], {
                        "request_id": info_riego["request_id"], "id_ws": id_ws,
                        "evento": "INTERRUMPIDO", "motivo": "Fuga detectada durante el riego.",
                    })
                    with lock_riegos:
                        riegos_activos.pop(id_ws, None)

            elif evento == "FUGA_RESUELTA":
                if not info_riego:
                    db.set_estado(id_ws, "DISPONIBLE")
                db.log_evento(f"Averia resuelta en {id_ws}. Estacion disponible de nuevo.")

            elif evento == "DESCONECTADO":
                db.marcar_desconectada(id_ws)
                db.log_evento(f"{id_ws} se ha desconectado de CENTRAL.")
                if info_riego:
                    responder_operario(info_riego["id_operador"], {
                        "request_id": info_riego["request_id"], "id_ws": id_ws,
                        "evento": "INTERRUMPIDO", "motivo": "La estacion se ha desconectado.",
                    })
                    with lock_riegos:
                        riegos_activos.pop(id_ws, None)

            elif evento == "CONECTADO":
                db.set_estado(id_ws, "DISPONIBLE")
                db.log_evento(f"{id_ws} reconectada a CENTRAL.")
        time.sleep(0.2)


def hilo_broadcast(intervalo: float = 2.0):
    """Publica periodicamente el estado global de la red (lo usan los WM_FO)."""
    while True:
        estaciones = db.get_todas_estaciones()
        productor.send(TOPIC_BROADCAST, value={"estaciones": estaciones, "timestamp": time.time()})
        productor.flush()
        time.sleep(intervalo)


# ============================================================== ACCIONES ===
# API interna usada por el dashboard web (botones de CENTRAL, punto 11 del enunciado)
def accion_iniciar_riego(id_ws: str, duracion_seg: int = 60):
    estacion = db.get_estacion(id_ws)
    if not estacion or estacion["estado"] not in ESTADOS_QUE_PERMITEN_RIEGO:
        return False, "La estacion no esta disponible para iniciar riego."
    with lock_riegos:
        riegos_activos[id_ws] = {"request_id": str(uuid.uuid4()), "id_operador": "CENTRAL",
                                  "duracion_seg": duracion_seg}
    db.set_estado(id_ws, "REGANDO", operario="CENTRAL")
    enviar_comando(id_ws, "INICIAR_RIEGO", duracion_seg=duracion_seg, operario="CENTRAL")
    db.log_evento(f"Riego iniciado manualmente en {id_ws} desde CENTRAL.")
    return True, "Riego iniciado."


def accion_bloquear(id_ws: str):
    enviar_comando(id_ws, "BLOQUEAR")
    db.set_estado(id_ws, "FUERA_DE_SERVICIO")
    with lock_riegos:
        riegos_activos.pop(id_ws, None)
    db.log_evento(f"{id_ws} puesta FUERA DE SERVICIO por CENTRAL.")
    return True, "Estacion bloqueada."


def accion_activar(id_ws: str):
    enviar_comando(id_ws, "ACTIVAR")
    db.set_estado(id_ws, "DISPONIBLE")
    db.log_evento(f"{id_ws} ACTIVADA de nuevo por CENTRAL.")
    return True, "Estacion activada."


# ================================================================= MAIN ====
def main():
    global db, productor

    puerto_sockets = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("SOCKET_PORT", 5000))
    kafka_broker = sys.argv[2] if len(sys.argv) > 2 else os.getenv("KAFKA_BROKER", "localhost:9093")
    puerto_web = int(sys.argv[3]) if len(sys.argv) > 3 else int(os.getenv("WEB_PORT", 8080))
    ruta_bd = os.getenv("DB_PATH", os.path.join(dir_cursor, "wm_central.db"))

    print("=" * 60)
    print(" WM_Central - WaterManagement")
    print(f" Puerto sockets : {puerto_sockets}")
    print(f" Kafka broker   : {kafka_broker}")
    print(f" Puerto web     : {puerto_web}")
    print(f" Base de datos  : {ruta_bd}")
    print("=" * 60)

    db = Database(ruta_bd)
    productor = crear_productor(kafka_broker)

    threading.Thread(target=servidor_sockets, args=(puerto_sockets,), daemon=True).start()
    threading.Thread(target=hilo_peticiones, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_telemetria, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_estado_ws, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_broadcast, daemon=True).start()

    # El dashboard web se importa aqui para evitar dependencias circulares
    from dashboard import crear_app
    app = crear_app(db, accion_iniciar_riego, accion_bloquear, accion_activar)
    app.run(host="0.0.0.0", port=puerto_web, threaded=True)


if __name__ == "__main__":
    main()
