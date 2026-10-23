# WM_WS/WM_WS_M.py
"""
WM_WS_M - Monitor de una estacion de riego (Watering Station).

Papel de intermediario:
-----------------------
El Monitor actúa como puente entre WM_Central y el Engine de su estación.
Tiene DOS canales de comunicación simultáneos:
  1. Canal hacia CENTRAL (cliente TCP): protocolo STX/ETX/LRC para el registro
     inicial y los latidos periódicos.
  2. Canal hacia el Engine (servidor TCP): protocolo STX/ETX/LRC para el
     handshake de identidad y los latidos de comprobación de salud (PING/OK/KO).
  3. Canal Kafka (productor): publica eventos de salud (FUGA, FUGA_RESUELTA,
     CONECTADO, DESCONECTADO) hacia CENTRAL cuando el estado del Engine cambia.

Diagrama de flujo:
  CENTRAL <--sockets (una sola vez + latidos)--> MONITOR <--sockets (continuo)--> ENGINE
                                                     |
                                                     +--Kafka 'estado-ws'--> CENTRAL

Protocolo de registro (flujo completo):
  ENQ -> ACK -> trama REGISTRO -> ACK -> trama REGISTRO_OK

¿Por qué se deja el socket abierto tras el registro?
Porque la presencia de la conexión TCP indica que el Monitor sigue vivo.
Si CENTRAL detecta que el socket se cierra (o que el Monitor deja de mandar
latidos ENQ), sabe inmediatamente que la estación está sin supervisión y la
marca DESCONECTADA.

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
#   CENTRAL <--sockets (una sola vez)--> MONITOR <--sockets (continuo)--> ENGINE
#                                            |
#                                            +--Kafka 'estado-ws'--> CENTRAL
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

# Intervalo (segundos) entre latidos PING enviados al Engine.
# Reducir este valor detecta fugas más rápido pero genera más tráfico de red.
# Con 1.0 s la detección de fuga tarda como máximo HEARTBEAT_TIMEOUT + 1 s.
HEARTBEAT_INTERVALO = 1.0

# Tiempo máximo (segundos) de espera de respuesta del Engine a cada PING.
# Si el Engine no contesta en 3 s, se considera avería (FUGA).
# Aumentar este valor hace el sistema más tolerante a latencia de red,
# pero también retrasa la detección de fallos reales.
HEARTBEAT_TIMEOUT = 3.0

# Cada cuantos segundos el Monitor manda un latido (ENQ) a CENTRAL por la conexion
# que mantiene abierta. CENTRAL da por desconectada la estacion si deja de recibirlos
# (TIMEOUT_MONITOR en WM_Central = 15 s, con LATIDO_CENTRAL = 5 s hay margen de 2 latidos).
# Si se reduce LATIDO_CENTRAL, CENTRAL detecta caídas más rápido pero hay más tráfico.
# Si se aumenta, hay que aumentar también TIMEOUT_MONITOR en WM_Central.
LATIDO_CENTRAL = 5.0


def registrar_en_central(ip_central, puerto_central, id_ws, ubicacion):
    """
    Conecta con WM_Central y ejecuta el protocolo de registro completo.

    Protocolo paso a paso:
      Paso 1: ENQ   → CENTRAL responde ACK  (handshake: "¿estás ahí?").
      Paso 2: trama REGISTRO#id_ws#ubicacion → CENTRAL responde ACK (LRC ok).
      Paso 3: CENTRAL envía trama REGISTRO_OK#id_ws (confirmación con nombre asignado).

    ¿Por qué se deja el socket abierto?
    La conexión TCP persistente es la señal de vida del Monitor.
    mantener_enlace_central() usa este mismo socket para enviar latidos ENQ
    periódicos. Si el socket se cierra (por caída del Monitor o de la red),
    CENTRAL lo detecta en atender_monitor() y marca la estación DESCONECTADA.

    Reintenta indefinidamente cada 5 s hasta lograrlo: es el comportamiento
    correcto porque CENTRAL puede arrancar más tarde que el Monitor.

    :returns: Socket TCP abierto y autenticado contra CENTRAL.
    """
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
            # La conexion NO se cierra: queda abierta y mantener_enlace_central() manda
            # latidos por ella. Si el Monitor muere, CENTRAL ve el cierre y marca la WS
            # como DESCONECTADA.
            return cliente

        except Exception as e:
            print(f"[MONITOR {id_ws}] No se pudo registrar en CENTRAL ({e}). Reintentando en 5s...")
            time.sleep(5)


def sincronizar_estado_con_central(productor, id_ws):
    """
    Informa a CENTRAL del estado real del Engine tras un (re)registro.

    ¿Por qué es necesario?
    Cuando el Monitor se vuelve a registrar (porque CENTRAL se reinició o la conexión
    se cayó), CENTRAL parte del estado DESCONECTADA para esa estación. Pero el Engine
    puede estar funcionando perfectamente: puede que haya un riego en curso, o que
    haya una fuga activa. Sin esta función, CENTRAL tendría información obsoleta hasta
    el próximo cambio de estado del Engine.

    Esta función lee el estado interno del Monitor (engine_conectado y estado_fuga) y
    publica los eventos Kafka correspondientes para que CENTRAL se actualice
    inmediatamente tras el re-registro.
    """
    with lock_estado:
        conectado, fuga = engine_conectado, estado_fuga
    if not conectado:
        return
    productor.send(TOPIC_ESTADO_WS, key=id_ws,
                   value={"id_ws": id_ws, "evento": "CONECTADO", "timestamp": time.time()})
    if fuga:
        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                       value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
    productor.flush()
    print(f"[MONITOR {id_ws}] Estado sincronizado con CENTRAL tras re-registrarse.")


def mantener_enlace_central(cliente, ip_central, puerto_central, id_ws, ubicacion, productor):
    """
    Hilo que mantiene viva la conexion con CENTRAL enviando latidos ENQ periódicos.

    Comportamiento:
      - Cada LATIDO_CENTRAL segundos (5 s por defecto) envía ENQ y espera ACK (10 s máx).
      - Si CENTRAL no responde (se reinició, red caída, etc.), cierra el socket
        y vuelve a registrar la estación llamando a registrar_en_central().
      - Después del re-registro, sincroniza el estado real del Engine con CENTRAL
        para que no haya diferencia entre lo que sabe CENTRAL y lo que hay realmente.

    ¿Qué pasa si CENTRAL no responde varios latidos seguidos?
    Con timeout=10 s y LATIDO_CENTRAL=5 s, un fallo se detecta en ~15 s.
    Entonces se reintenta el registro indefinidamente cada 5 s hasta reconectar.
    """
    while True:
        time.sleep(LATIDO_CENTRAL)
        try:
            cliente.sendall(ENQ)
            if recv_exact_frame(cliente, timeout=10) != ACK:
                raise ConnectionError("CENTRAL no respondio ACK al latido.")
        except Exception as e:
            print(f"[MONITOR {id_ws}] Enlace con CENTRAL perdido ({e}). Re-registrando...")
            try:
                cliente.close()
            except Exception:
                pass
            cliente = registrar_en_central(ip_central, puerto_central, id_ws, ubicacion)
            sincronizar_estado_con_central(productor, id_ws)


# --------------------------------------------------------- enlace con Engine
# Indica si ahora mismo hay una fuga notificada. Sirve para avisar a CENTRAL solo
# al CAMBIAR de estado (no en cada latido), y esta protegido con un lock.
estado_fuga = False
# Indica si hay un Engine conectado ahora mismo (para poder contarle a CENTRAL el
# estado real cuando la estacion se vuelve a registrar).
engine_conectado = False
lock_estado = threading.Lock()


def atender_engine(conn, addr, id_ws, ubicacion, productor):
    global estado_fuga, engine_conectado
    print(f"[MONITOR {id_ws}] Engine conectado desde {addr}")

    # Handshake: informamos al Engine de su identidad
    conn.sendall(empaquetar_trama(f"IDENTIDAD#{id_ws}#{ubicacion}"))

    # Al conectar un Engine nuevo se parte de "sin fuga".
    with lock_estado:
        estado_fuga = False
        engine_conectado = True

    try:
        while True:
            # Latido: enviamos PING y esperamos la respuesta hasta HEARTBEAT_TIMEOUT.
            conn.sendall(empaquetar_trama("PING"))
            trama = recv_exact_frame(conn, timeout=HEARTBEAT_TIMEOUT)
            valido, respuesta = (False, "") if not trama else desempaquetar_trama(trama)

            # CASO A: sin respuesta (trama vacía = conexión cerrada) o trama inválida.
            # Se interpreta como avería: el Engine no puede responder, algo va mal.
            # Solo notificamos FUGA la primera vez (transición normal → fuga) para no
            # saturar el topic Kafka con mensajes repetidos si el Engine sigue sin responder.
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

            # CASO B: el Engine responde KO (fuga simulada con la tecla 'k' en la consola).
            # Igual que el caso A: solo notificamos la transición normal → fuga.
            if respuesta == "KO":
                with lock_estado:
                    if not estado_fuga:
                        estado_fuga = True
                        productor.send(TOPIC_ESTADO_WS, key=id_ws,
                                        value={"id_ws": id_ws, "evento": "FUGA", "timestamp": time.time()})
                        productor.flush()
                        print(f"[MONITOR {id_ws}] >>> FUGA detectada por el Engine: notificada a CENTRAL.")
            # CASO C: el Engine responde OK. Si antes había fuga, significa que la avería
            # se ha resuelto: notificamos FUGA_RESUELTA para que CENTRAL vuelva a poner
            # la estación en DISPONIBLE.
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
        with lock_estado:
            engine_conectado = False
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
        # atender_engine() es BLOQUEANTE: no vuelve hasta que el Engine se desconecta.
        # Esto garantiza que cada Monitor gestiona como máximo UN Engine a la vez,
        # lo que es el comportamiento correcto para una estación física: hay una sola
        # electroválvula y un solo sensor de caudal. Cuando el Engine se desconecta,
        # el bucle while vuelve a accept() para esperar una nueva conexión.
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
    enlace = registrar_en_central(ip_central, puerto_central, id_ws, ubicacion)
    productor = crear_productor(kafka_broker)
    threading.Thread(
        target=mantener_enlace_central,
        args=(enlace, ip_central, puerto_central, id_ws, ubicacion, productor),
        daemon=True,
    ).start()

    servidor_para_engine(puerto_engine, id_ws, ubicacion, productor)
