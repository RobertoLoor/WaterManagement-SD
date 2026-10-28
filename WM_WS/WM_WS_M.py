# WM_WS/WM_WS_M.py
"""
Monitor de estación: actúa como intermediario entre Central y el Engine local.
Registra la estación en Central por sockets TCP manteniendo latidos periódicos,
supervisa la salud del Engine mediante PINGs locales y reporta averías por Kafka.
"""
import os
import sys
import socket
import threading
import time

# Busca dinámicamente la carpeta raíz del proyecto para importar las utilidades compartidas.
_inicio = os.path.dirname(os.path.abspath(__file__))
dir_cursor = _inicio
while True:
    if os.path.isdir(os.path.join(dir_cursor, "utils")):
        if dir_cursor not in sys.path:
            sys.path.insert(0, dir_cursor)
        break
    parent = os.path.dirname(dir_cursor)
    if parent == dir_cursor:
        raise RuntimeError(f"No se encontro utils/. Inicio={_inicio}")
    dir_cursor = parent

from utils.protocol import empaquetar_trama, desempaquetar_trama, recv_exact_frame, ENQ, ACK
from utils.kafka_utils import crear_productor, TOPIC_ESTADO_WS

# Constantes de tiempos en segundos para los latidos y detección de fallos
HEARTBEAT_INTERVALO = 1.0  # Frecuencia con la que enviamos PING al Engine local
HEARTBEAT_TIMEOUT = 3.0    # Tiempo máximo de espera a la respuesta del Engine antes de declarar avería
LATIDO_CENTRAL = 5.0       # Frecuencia con la que enviamos ENQ a Central para avisar de que seguimos online

# Variables de estado protegidas por un Lock para coordinar el estado entre hilos
estado_fuga = False
engine_conectado = False
lock_estado = threading.Lock()


# Establece la conexión socket con Central y completa el diálogo de registro inicial.
def registrar_en_central(ip_central, puerto_central, id_ws, ubicacion):
    while True:
        cliente = None
        try:
            # Crea un socket TCP cliente (AF_INET = IPv4, SOCK_STREAM = TCP)
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cliente.settimeout(10)
            cliente.connect((ip_central, puerto_central))
            print(f"[MONITOR {id_ws}] Conectado a CENTRAL en {ip_central}:{puerto_central}")

            # Diálogo de control: enviamos ENQ ("¿estás listo?") y esperamos ACK ("sí")
            cliente.sendall(ENQ)
            if recv_exact_frame(cliente) != ACK:
                raise ConnectionError("CENTRAL no respondio ACK al ENQ.")

            # Envía la trama con los datos de registro (id y ubicación)
            mensaje = f"REGISTRO#{id_ws}#{ubicacion}"
            cliente.sendall(empaquetar_trama(mensaje))

            # Espera confirmación ACK de recepción de la trama
            if recv_exact_frame(cliente) != ACK:
                raise ConnectionError("CENTRAL no confirmo la recepcion del registro.")

            # Recibe la respuesta formal de registro (REGISTRO_OK#id_ws)
            trama_respuesta = recv_exact_frame(cliente)
            valido, msg = desempaquetar_trama(trama_respuesta)
            if valido:
                print(f"[MONITOR {id_ws}] Respuesta de CENTRAL: {msg}")
            return cliente

        except Exception as e:
            if cliente:
                try:
                    cliente.close()
                except Exception:
                    pass
            print(f"[MONITOR {id_ws}] No se pudo registrar en CENTRAL ({e}). Reintentando en 5s...")
            time.sleep(5)


# Reenvía el estado actual de la estación a Kafka tras restablecer la conexión con Central.
def sincronizar_estado_con_central(productor, id_ws):
    with lock_estado:
        conectado, fuga = engine_conectado, estado_fuga
    if not conectado:
        return
    # Notifica que el Engine está conectado y, si había una fuga previa, vuelve a avisar de ella.
    productor.send(TOPIC_ESTADO_WS, key=id_ws,
                   value={"id_ws": id_ws, "evento": "CONECTADO", "timestamp": time.time()})
    if fuga:
        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                       value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
    productor.flush()
    print(f"[MONITOR {id_ws}] Estado sincronizado con CENTRAL tras re-registrarse.")


# Hilo en bucle que envía latidos periódicos (ENQ) a Central para mantener vivo el enlace TCP.
def mantener_enlace_central(cliente, ip_central, puerto_central, id_ws, ubicacion, productor):
    while True:
        time.sleep(LATIDO_CENTRAL)
        try:
            # Envía el byte ENQ a través del socket TCP que mantuvimos abierto.
            cliente.sendall(ENQ)
            if recv_exact_frame(cliente, timeout=10) != ACK:
                raise ConnectionError("CENTRAL no respondio ACK al latido.")
        except Exception as e:
            print(f"[MONITOR {id_ws}] Enlace con CENTRAL perdido ({e}). Re-registrando...")
            try:
                cliente.close()
            except Exception:
                pass
            # Si el enlace se rompió, reintenta registrarse desde cero hasta volver a conectar.
            cliente = registrar_en_central(ip_central, puerto_central, id_ws, ubicacion)
            try:
                sincronizar_estado_con_central(productor, id_ws)
            except Exception as e_sync:
                print(f"[MONITOR {id_ws}][ERROR] Error sincronizando estado: {e_sync}")


# Supervisa al Engine local mediante PINGs periódicos para detectar fugas o desconexiones.
def atender_engine(conn, addr, id_ws, ubicacion, productor):
    global estado_fuga, engine_conectado
    print(f"[MONITOR {id_ws}] Engine conectado desde {addr}")

    try:
        # Lo primero que hacemos al conectar el Engine es enviarle su identidad (id y ubicación).
        conn.sendall(empaquetar_trama(f"IDENTIDAD#{id_ws}#{ubicacion}"))

        with lock_estado:
            estado_fuga = False
            engine_conectado = True

        # Bucle de vigilancia de salud (heartbeat): mandamos PING cada segundo.
        while True:
            conn.sendall(empaquetar_trama("PING"))
            # Espera la respuesta dentro de HEARTBEAT_TIMEOUT (3 segundos).
            trama = recv_exact_frame(conn, timeout=HEARTBEAT_TIMEOUT)
            valido, respuesta = (False, "") if not trama else desempaquetar_trama(trama)

            # Si no responde o la trama es inválida, se interpreta como avería/fuga por timeout.
            if not trama or not valido:
                with lock_estado:
                    if not estado_fuga:
                        estado_fuga = True
                        try:
                            productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                            value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
                            productor.flush()
                        except Exception as e_send:
                            print(f"[MONITOR {id_ws}][ERROR] Error enviando FUGA a Kafka: {e_send}")
                        print(f"[MONITOR {id_ws}] >>> Sin respuesta del Engine: FUGA notificada a CENTRAL.")
                if not trama:
                    break
                time.sleep(HEARTBEAT_INTERVALO)
                continue

            # Respuesta KO: el sensor del Engine reporta fuga activa.
            if respuesta == "KO":
                with lock_estado:
                    if not estado_fuga:
                        estado_fuga = True
                        try:
                            productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                            value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
                            productor.flush()
                        except Exception as e_send:
                            print(f"[MONITOR {id_ws}][ERROR] Error enviando FUGA a Kafka: {e_send}")
                        print(f"[MONITOR {id_ws}] >>> FUGA detectada por el Engine: notificada a CENTRAL.")
            # Respuesta OK: funcionamiento normal; si venía de una fuga, se resuelve la incidencia.
            elif respuesta == "OK":
                with lock_estado:
                    if estado_fuga:
                        estado_fuga = False
                        try:
                            productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                            value={"id_ws": id_ws, "evento": "FUGA_RESUELTA", "timestamp": time.time()})
                            productor.flush()
                        except Exception as e_send:
                            print(f"[MONITOR {id_ws}][ERROR] Error enviando FUGA_RESUELTA a Kafka: {e_send}")
                        print(f"[MONITOR {id_ws}] >>> Averia resuelta: CENTRAL notificada.")

            time.sleep(HEARTBEAT_INTERVALO)

    except Exception as e:
        print(f"[MONITOR {id_ws}] Conexion con Engine interrumpida: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        with lock_estado:
            engine_conectado = False
        # Si el Engine se desconecta, lo notificamos inmediatamente a Central por Kafka.
        try:
            productor.send(TOPIC_ESTADO_WS, key=id_ws,
                            value={"id_ws": id_ws, "evento": "DESCONECTADO", "timestamp": time.time()})
            productor.flush()
            print(f"[MONITOR {id_ws}] Engine desconectado. Notificado a CENTRAL.")
        except Exception as e_k:
            print(f"[MONITOR {id_ws}][ERROR] Error notificando DESCONECTADO a Kafka: {e_k}")


# Servidor socket TCP local donde espera que el proceso Engine se conecte.
def servidor_para_engine(puerto_engine, id_ws, ubicacion, productor):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Permite reiniciar sin que el puerto quede pillado por el sistema operativo.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', puerto_engine))
    s.listen()
    print(f"[MONITOR {id_ws}] Esperando conexion del Engine en el puerto {puerto_engine}...")
    while True:
        try:
            # Espera a que el Engine local se conecte mediante TCP.
            conn, addr = s.accept()
            try:
                productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                value={"id_ws": id_ws, "evento": "CONECTADO", "timestamp": time.time()})
                productor.flush()
            except Exception as e_k:
                print(f"[MONITOR {id_ws}][ERROR] Error notificando CONECTADO a Kafka: {e_k}")
            atender_engine(conn, addr, id_ws, ubicacion, productor)
        except Exception as e:
            print(f"[MONITOR {id_ws}][ERROR] Excepcion en servidor_para_engine: {e}")
            time.sleep(1)


if __name__ == "__main__":
    # Lee parámetros desde la línea de comandos o variables de entorno del sistema operativo.
    ip_central = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CENTRAL_HOST", "127.0.0.1")
    try:
        puerto_central = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("CENTRAL_PORT", "5000"))
    except ValueError:
        puerto_central = 5000

    id_ws = sys.argv[3] if len(sys.argv) > 3 else os.getenv("WS_ID", "WS-01")
    ubicacion = sys.argv[4] if len(sys.argv) > 4 else os.getenv("WS_UBICACION", "Parque Canalejas")

    try:
        puerto_engine = int(sys.argv[5]) if len(sys.argv) > 5 else int(os.getenv("ENGINE_PORT", "6001"))
    except ValueError:
        puerto_engine = 6001

    kafka_broker = sys.argv[6] if len(sys.argv) > 6 else os.getenv("KAFKA_BROKER", "localhost:9093")

    print(f"[MONITOR {id_ws}] Iniciando... ubicacion='{ubicacion}' puerto_engine={puerto_engine}")

    enlace = registrar_en_central(ip_central, puerto_central, id_ws, ubicacion)
    productor = crear_productor(kafka_broker)

    # Arranca el hilo de latidos hacia Central en segundo plano.
    threading.Thread(
        target=mantener_enlace_central,
        args=(enlace, ip_central, puerto_central, id_ws, ubicacion, productor),
        daemon=True,
    ).start()

    # Mantiene el servidor socket para atender al Engine local.
    servidor_para_engine(puerto_engine, id_ws, ubicacion, productor)
