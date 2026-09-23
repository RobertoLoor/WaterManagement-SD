# WM_WS/WM_WS_M.py
"""
WM_WS_M - Monitor de una estacion de riego (Watering Station).

Responsabilidades:
 - Registrarse/autenticarse contra WM_Central via sockets (protocolo
   STX/ETX/LRC), enviando su Id y ubicacion.
 - Actuar como servidor de sockets para el Engine (WM_WS_E) de su propia
   estacion: le informa de su Id/ubicacion y le envia un "latido" de
   comprobacion de salud cada segundo.
 - Si el Engine no responde o responde KO, se interpreta como una fuga y
   se notifica a CENTRAL (via Kafka). Cuando vuelve a responder OK, se
   notifica la resolucion de la averia.

Uso:
    python WM_WS_M.py <ip_central> <puerto_central> <id_ws> <ubicacion> <puerto_engine> <kafka_broker>
"""
import os
import sys
import socket
import threading
import time

dir_cursor = os.path.dirname(os.path.abspath(__file__))
while True:
    utils_dir = os.path.join(dir_cursor, "utils")
    env_file = os.path.join(dir_cursor, ".env")
    if os.path.isdir(utils_dir) and os.path.isfile(env_file):
        if dir_cursor not in sys.path:
            sys.path.insert(0, dir_cursor)
        break
    parent = os.path.dirname(dir_cursor)
    if parent == dir_cursor:
        raise RuntimeError("No se encontro la raiz del proyecto (utils/ + .env).")
    dir_cursor = parent

from utils.protocol import empaquetar_trama, desempaquetar_trama, recv_exact_frame, ENQ, ACK, NACK
from utils.kafka_utils import crear_productor, TOPIC_ESTADO_WS

HEARTBEAT_INTERVALO = 1.0
HEARTBEAT_TIMEOUT = 3.0


def registrar_en_central(ip_central, puerto_central, id_ws, ubicacion) -> bool:
    """Se conecta a WM_Central y envia la trama de registro por sockets. Reintenta hasta lograrlo."""
    while True:
        try:
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cliente.settimeout(10)
            cliente.connect((ip_central, puerto_central))
            print(f"[MONITOR {id_ws}] Conectado a CENTRAL en {ip_central}:{puerto_central}")

            cliente.sendall(ENQ)
            if recv_exact_frame(cliente) != ACK:
                cliente.close()
                raise ConnectionError("CENTRAL no respondio ACK al ENQ.")

            mensaje = f"REGISTRO#{id_ws}#{ubicacion}"
            cliente.sendall(empaquetar_trama(mensaje))

            if recv_exact_frame(cliente) != ACK:
                cliente.close()
                raise ConnectionError("CENTRAL no confirmo la recepcion del registro.")

            trama_respuesta = recv_exact_frame(cliente)
            valido, msg = desempaquetar_trama(trama_respuesta)
            if valido:
                print(f"[MONITOR {id_ws}] Respuesta de CENTRAL: {msg}")
            cliente.close()
            return True

        except Exception as e:
            print(f"[MONITOR {id_ws}] No se pudo registrar en CENTRAL ({e}). Reintentando en 5s...")
            time.sleep(5)


# --------------------------------------------------------- enlace con Engine
estado_fuga = False
lock_estado = threading.Lock()


def atender_engine(conn, addr, id_ws, ubicacion, productor):
    global estado_fuga
    print(f"[MONITOR {id_ws}] Engine conectado desde {addr}")

    # Handshake: informamos al Engine de su identidad
    conn.sendall(empaquetar_trama(f"IDENTIDAD#{id_ws}#{ubicacion}"))

    with lock_estado:
        estado_fuga = False

    try:
        while True:
            conn.sendall(empaquetar_trama("PING"))
            trama = recv_exact_frame(conn, timeout=HEARTBEAT_TIMEOUT)
            valido, respuesta = (False, "") if not trama else desempaquetar_trama(trama)

            if not trama or not valido:
                # Sin respuesta: se interpreta como averia
                with lock_estado:
                    if not estado_fuga:
                        estado_fuga = True
                        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                        value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
                        productor.flush()
                        print(f"[MONITOR {id_ws}] >>> Sin respuesta del Engine: FUGA notificada a CENTRAL.")
                if not trama:
                    break  # conexion cerrada, salimos para intentar reconectar
                time.sleep(HEARTBEAT_INTERVALO)
                continue

            if respuesta == "KO":
                with lock_estado:
                    if not estado_fuga:
                        estado_fuga = True
                        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                        value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
                        productor.flush()
                        print(f"[MONITOR {id_ws}] >>> FUGA detectada por el Engine: notificada a CENTRAL.")
            elif respuesta == "OK":
                with lock_estado:
                    if estado_fuga:
                        estado_fuga = False
                        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                        value={"id_ws": id_ws, "evento": "FUGA_RESUELTA", "timestamp": time.time()})
                        productor.flush()
                        print(f"[MONITOR {id_ws}] >>> Averia resuelta: CENTRAL notificada.")

            time.sleep(HEARTBEAT_INTERVALO)

    except Exception as e:
        print(f"[MONITOR {id_ws}] Conexion con Engine interrumpida: {e}")
    finally:
        conn.close()
        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                        value={"id_ws": id_ws, "evento": "DESCONECTADO", "timestamp": time.time()})
        productor.flush()
        print(f"[MONITOR {id_ws}] Engine desconectado. Notificado a CENTRAL.")


def servidor_para_engine(puerto_engine, id_ws, ubicacion, productor):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', puerto_engine))
    s.listen()
    print(f"[MONITOR {id_ws}] Esperando conexion del Engine en el puerto {puerto_engine}...")
    while True:
        conn, addr = s.accept()
        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                        value={"id_ws": id_ws, "evento": "CONECTADO", "timestamp": time.time()})
        productor.flush()
        atender_engine(conn, addr, id_ws, ubicacion, productor)  # bloqueante: 1 engine por WS


if __name__ == "__main__":
    ip_central = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CENTRAL_HOST", "127.0.0.1")
    puerto_central = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("CENTRAL_PORT", 5000))
    id_ws = sys.argv[3] if len(sys.argv) > 3 else os.getenv("WS_ID", "WS-01")
    ubicacion = sys.argv[4] if len(sys.argv) > 4 else os.getenv("WS_UBICACION", "Parque Canalejas")
    puerto_engine = int(sys.argv[5]) if len(sys.argv) > 5 else int(os.getenv("ENGINE_PORT", 6001))
    kafka_broker = sys.argv[6] if len(sys.argv) > 6 else os.getenv("KAFKA_BROKER", "localhost:9093")

    print(f"[MONITOR {id_ws}] Iniciando... ubicacion='{ubicacion}' puerto_engine={puerto_engine}")

    registrar_en_central(ip_central, puerto_central, id_ws, ubicacion)
    productor = crear_productor(kafka_broker)

    servidor_para_engine(puerto_engine, id_ws, ubicacion, productor)
