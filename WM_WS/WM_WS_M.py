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

# ---------------------------------------------------------------------------
# VISION GENERAL PARA ESTUDIAR
# ---------------------------------------------------------------------------
# El Monitor es un "intermediario" con dos caras:
#
#   CENTRAL <--sockets (una sola vez)-->  MONITOR  <--sockets (continuo)-->  ENGINE
#                                             |
#                                             +--Kafka 'estado-ws'--> CENTRAL
#
#  1) Al arrancar se REGISTRA en CENTRAL por sockets (ENQ/ACK -> REGISTRO -> ACK -> REGISTRO_OK).
#  2) Despues abre un servidor en 'puerto_engine' y espera a su Engine.
#  3) Cuando el Engine conecta: le manda IDENTIDAD#id#ubicacion y luego un PING cada 1 s.
#       - Engine responde "OK"       -> todo bien
#       - Engine responde "KO"       -> FUGA (aviso a CENTRAL por Kafka)
#       - Engine no responde en 3 s  -> FUGA tambien
#       - Vuelve "OK" tras una fuga  -> FUGA_RESUELTA
#       - Se cierra la conexion      -> DESCONECTADO
# ---------------------------------------------------------------------------

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

from utils.protocol import empaquetar_trama, desempaquetar_trama, recv_exact_frame, ENQ, ACK, NACK
from utils.kafka_utils import crear_productor, TOPIC_ESTADO_WS

# Cada cuantos segundos se pregunta al Engine, y cuanto se espera su respuesta.
HEARTBEAT_INTERVALO = 1.0
HEARTBEAT_TIMEOUT = 3.0


def registrar_en_central(ip_central, puerto_central, id_ws, ubicacion) -> bool:
    """Se conecta a WM_Central y envia la trama de registro por sockets. Reintenta hasta lograrlo."""
    # Bucle de reintentos: si CENTRAL no esta accesible, se vuelve a intentar cada 5 s.
    while True:
        try:
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cliente.settimeout(10)   # maximo 10 s esperando a conectar o a recibir
            cliente.connect((ip_central, puerto_central))
            print(f"[MONITOR {id_ws}] Conectado a CENTRAL en {ip_central}:{puerto_central}")

            # Paso 1: ENQ ("¿estas ahi?") -> CENTRAL debe responder ACK.
            cliente.sendall(ENQ)
            if recv_exact_frame(cliente) != ACK:
                cliente.close()
                raise ConnectionError("CENTRAL no respondio ACK al ENQ.")

            # Paso 2: enviamos la trama de registro: REGISTRO#WS-01#River Park
            mensaje = f"REGISTRO#{id_ws}#{ubicacion}"
            cliente.sendall(empaquetar_trama(mensaje))

            # Paso 3: CENTRAL confirma que la trama llego bien con ACK.
            if recv_exact_frame(cliente) != ACK:
                cliente.close()
                raise ConnectionError("CENTRAL no confirmo la recepcion del registro.")

            # Paso 4: CENTRAL responde con otra trama (REGISTRO_OK#WS-01).
            trama_respuesta = recv_exact_frame(cliente)
            valido, msg = desempaquetar_trama(trama_respuesta)
            if valido:
                print(f"[MONITOR {id_ws}] Respuesta de CENTRAL: {msg}")
            # El registro es una conversacion corta: se cierra la conexion y se sale del bucle.
            cliente.close()
            return True

        except Exception as e:
            print(f"[MONITOR {id_ws}] No se pudo registrar en CENTRAL ({e}). Reintentando en 5s...")
            time.sleep(5)


# --------------------------------------------------------- enlace con Engine
# Indica si ahora mismo hay una fuga notificada. Sirve para avisar a CENTRAL solo
# al CAMBIAR de estado (no en cada latido), y esta protegido con un lock.
estado_fuga = False
lock_estado = threading.Lock()


def atender_engine(conn, addr, id_ws, ubicacion, productor):
    global estado_fuga
    print(f"[MONITOR {id_ws}] Engine conectado desde {addr}")

    # Handshake: informamos al Engine de su identidad
    conn.sendall(empaquetar_trama(f"IDENTIDAD#{id_ws}#{ubicacion}"))

    # Al conectar un Engine nuevo se parte de "sin fuga".
    with lock_estado:
        estado_fuga = False

    try:
        while True:
            # Latido: enviamos PING y esperamos la respuesta hasta HEARTBEAT_TIMEOUT.
            conn.sendall(empaquetar_trama("PING"))
            trama = recv_exact_frame(conn, timeout=HEARTBEAT_TIMEOUT)
            valido, respuesta = (False, "") if not trama else desempaquetar_trama(trama)

            # CASO A: no hay respuesta o la trama es invalida -> se interpreta como averia.
            if not trama or not valido:
                # Sin respuesta: se interpreta como averia
                with lock_estado:
                    if not estado_fuga:
                        # Solo notificamos la primera vez (transicion normal -> fuga).
                        estado_fuga = True
                        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                        value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
                        productor.flush()
                        print(f"[MONITOR {id_ws}] >>> Sin respuesta del Engine: FUGA notificada a CENTRAL.")
                if not trama:
                    break  # conexion cerrada, salimos para intentar reconectar
                time.sleep(HEARTBEAT_INTERVALO)
                continue

            # CASO B: el Engine responde KO (fuga simulada con la tecla 'k').
            if respuesta == "KO":
                with lock_estado:
                    if not estado_fuga:
                        estado_fuga = True
                        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                        value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
                        productor.flush()
                        print(f"[MONITOR {id_ws}] >>> FUGA detectada por el Engine: notificada a CENTRAL.")
            # CASO C: responde OK. Si antes habia fuga, se notifica que ya esta resuelta.
            elif respuesta == "OK":
                with lock_estado:
                    if estado_fuga:
                        estado_fuga = False
                        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                        value={"id_ws": id_ws, "evento": "FUGA_RESUELTA", "timestamp": time.time()})
                        productor.flush()
                        print(f"[MONITOR {id_ws}] >>> Averia resuelta: CENTRAL notificada.")

            # Esperamos 1 s hasta el siguiente latido.
            time.sleep(HEARTBEAT_INTERVALO)

    except Exception as e:
        print(f"[MONITOR {id_ws}] Conexion con Engine interrumpida: {e}")
    finally:
        # Salgamos por donde salgamos: cerramos el socket y avisamos a CENTRAL de que
        # la estacion queda DESCONECTADA.
        conn.close()
        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                        value={"id_ws": id_ws, "evento": "DESCONECTADO", "timestamp": time.time()})
        productor.flush()
        print(f"[MONITOR {id_ws}] Engine desconectado. Notificado a CENTRAL.")


def servidor_para_engine(puerto_engine, id_ws, ubicacion, productor):
    # Servidor TCP en el puerto del Engine (0.0.0.0 = todas las interfaces).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', puerto_engine))
    s.listen()
    print(f"[MONITOR {id_ws}] Esperando conexion del Engine en el puerto {puerto_engine}...")
    while True:
        conn, addr = s.accept()
        # Al aceptar un Engine avisamos a CENTRAL de que la estacion esta CONECTADA.
        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                        value={"id_ws": id_ws, "evento": "CONECTADO", "timestamp": time.time()})
        productor.flush()
        atender_engine(conn, addr, id_ws, ubicacion, productor)  # bloqueante: 1 engine por WS


if __name__ == "__main__":
    # Parametros: argumento de linea de comandos -> variable de entorno -> valor por defecto.
    ip_central = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CENTRAL_HOST", "127.0.0.1")
    puerto_central = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("CENTRAL_PORT", 5000))
    id_ws = sys.argv[3] if len(sys.argv) > 3 else os.getenv("WS_ID", "WS-01")
    ubicacion = sys.argv[4] if len(sys.argv) > 4 else os.getenv("WS_UBICACION", "Parque Canalejas")
    puerto_engine = int(sys.argv[5]) if len(sys.argv) > 5 else int(os.getenv("ENGINE_PORT", 6001))
    kafka_broker = sys.argv[6] if len(sys.argv) > 6 else os.getenv("KAFKA_BROKER", "localhost:9093")

    print(f"[MONITOR {id_ws}] Iniciando... ubicacion='{ubicacion}' puerto_engine={puerto_engine}")

    # Orden de arranque: 1) registrar en CENTRAL (sockets), 2) crear productor Kafka,
    # 3) quedarse esperando al Engine (esta funcion no termina nunca).
    registrar_en_central(ip_central, puerto_central, id_ws, ubicacion)
    productor = crear_productor(kafka_broker)

    servidor_para_engine(puerto_engine, id_ws, ubicacion, productor)