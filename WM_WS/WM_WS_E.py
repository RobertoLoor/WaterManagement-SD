# WM_WS/WM_WS_E.py
"""
WM_WS_E - Engine de una estacion de riego (Watering Station).

Responsabilidades:
 - Conectarse como cliente de sockets a su Monitor (WM_WS_M) para recibir
   su identidad (Id/ubicacion) y responder al latido de salud cada
   segundo (OK / KO). KO se simula manualmente pulsando 'k' en consola
   ('o' para resolver la averia).
 - Escuchar por Kafka los comandos que envia CENTRAL (iniciar/detener
   riego, reset del contador, bloquear/activar la estacion).
 - Durante el riego, abrir la "electrovalvula" (simulada) y enviar
   telemetria (caudal y volumen acumulado) cada segundo hasta que se
   cumpla el tiempo establecido, se reciba orden de parar, o se detecte
   una fuga.

Uso:
    python WM_WS_E.py <ip_monitor> <puerto_monitor> <kafka_broker>

Controles de teclado en consola:
    k + Enter  -> simular avería / fuga (KO)
    o + Enter  -> resolver avería (OK)
    q + Enter  -> salir
"""
import os
import sys
import socket
import threading
import time
import random

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

from utils.protocol import empaquetar_trama, desempaquetar_trama, recv_exact_frame
from utils.kafka_utils import crear_productor, crear_consumidor, TOPIC_TELEMETRIA, TOPIC_COMANDOS

# --------------------------------------------------------------- estado ---
id_ws = None
ubicacion = None
en_fuga = False
fuera_de_servicio = False
lock_estado = threading.Lock()

riego_activo = threading.Event()
riego_stop_solicitado = threading.Event()
volumen_acumulado = 0.0
operario_actual = None


# ---------------------------------------------------- enlace con Monitor --
def hilo_monitor(ip_monitor, puerto_monitor, identidad_lista: threading.Event):
    """Se conecta al Monitor, recibe su identidad y responde a los latidos (PING)."""
    global id_ws, ubicacion
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip_monitor, puerto_monitor))
            print(f"[ENGINE] Conectado al Monitor en {ip_monitor}:{puerto_monitor}")

            trama = recv_exact_frame(sock, timeout=10)
            valido, mensaje = desempaquetar_trama(trama) if trama else (False, "")
            if valido and mensaje.startswith("IDENTIDAD#"):
                partes = mensaje.split('#')
                id_ws, ubicacion = partes[1], partes[2]
                print(f"[ENGINE] Identidad recibida: {id_ws} @ {ubicacion}")
                identidad_lista.set()

            while True:
                trama = recv_exact_frame(sock, timeout=15)
                if not trama:
                    raise ConnectionError("Monitor desconectado.")
                valido, mensaje = desempaquetar_trama(trama)
                if not valido or mensaje != "PING":
                    continue
                with lock_estado:
                    respuesta = "KO" if en_fuga else "OK"
                sock.sendall(empaquetar_trama(respuesta))

        except Exception as e:
            print(f"[ENGINE] Conexion con Monitor perdida ({e}). Reintentando en 3s...")
            time.sleep(3)


def hilo_teclado():
    """Permite simular una averia/fuga (k) o resolverla (o) desde consola."""
    global en_fuga
    print("[ENGINE] Pulsa 'k' + Enter para simular una fuga, 'o' + Enter para resolverla.")
    for linea in sys.stdin:
        tecla = linea.strip().lower()
        if tecla == 'k':
            with lock_estado:
                en_fuga = True
            print("[ENGINE] >>> Fuga simulada activada (KO).")
        elif tecla == 'o':
            with lock_estado:
                en_fuga = False
            print("[ENGINE] >>> Averia resuelta (OK).")
        elif tecla == 'q':
            print("[ENGINE] Saliendo...")
            os._exit(0)


# ------------------------------------------------------------ riego -------
def hilo_riego(productor, duracion_seg: int, operario: str):
    """Simula el riego: abre la valvula y envia telemetria cada segundo."""
    global volumen_acumulado, operario_actual
    volumen_acumulado = 0.0
    operario_actual = operario
    riego_activo.set()
    riego_stop_solicitado.clear()
    inicio = time.time()
    motivo_fin = "TIEMPO_AGOTADO"
    print(f"[ENGINE {id_ws}] >>> Electrovalvula ABIERTA. Riego durante {duracion_seg}s (operario: {operario}).")

    while True:
        with lock_estado:
            fuga_actual = en_fuga
            bloqueada = fuera_de_servicio

        if fuga_actual:
            motivo_fin = "FUGA"
            break
        if bloqueada:
            motivo_fin = "BLOQUEO"
            break
        if riego_stop_solicitado.is_set():
            motivo_fin = "MANUAL"
            break
        if time.time() - inicio >= duracion_seg:
            motivo_fin = "TIEMPO_AGOTADO"
            break

        caudal = round(random.uniform(10.0, 25.0), 2)
        volumen_acumulado = round(volumen_acumulado + caudal / 60.0, 2)

        productor.send(TOPIC_TELEMETRIA, key=id_ws, value={
            "tipo": "TELEMETRIA", "id_ws": id_ws, "caudal_lmin": caudal,
            "volumen_total_l": volumen_acumulado, "timestamp": time.time(),
        })
        print(f"[ENGINE {id_ws}] Regando... caudal={caudal} L/min | acumulado={volumen_acumulado} L")
        time.sleep(1)

    duracion_real = round(time.time() - inicio, 1)
    print(f"[ENGINE {id_ws}] >>> Electrovalvula CERRADA. Motivo: {motivo_fin}. Volumen total: {volumen_acumulado} L.")
    productor.send(TOPIC_TELEMETRIA, key=id_ws, value={
        "tipo": "RIEGO_FINALIZADO", "id_ws": id_ws, "operario": operario_actual,
        "motivo": motivo_fin, "volumen_total_l": volumen_acumulado,
        "duracion_seg": duracion_real, "timestamp": time.time(),
    })
    productor.flush()
    riego_activo.clear()
    operario_actual = None


def hilo_comandos(kafka_broker, identidad_lista: threading.Event):
    """Escucha los comandos que envia CENTRAL (iniciar/detener riego, bloquear, activar...)."""
    global fuera_de_servicio
    identidad_lista.wait()  # necesitamos conocer nuestro id_ws antes de filtrar comandos

    productor = crear_productor(kafka_broker)
    consumidor = crear_consumidor(TOPIC_COMANDOS, kafka_broker, group_id=f"engine-{id_ws}")
    print(f"[ENGINE {id_ws}] Escuchando comandos en '{TOPIC_COMANDOS}'...")

    while True:
        for msg in consumidor:
            data = msg.value
            destino = data.get("id_ws")
            if destino not in (id_ws, "ALL"):
                continue

            comando = data.get("comando")

            if comando == "INICIAR_RIEGO":
                with lock_estado:
                    bloqueada = fuera_de_servicio
                if riego_activo.is_set() or bloqueada:
                    print(f"[ENGINE {id_ws}] Orden INICIAR_RIEGO ignorada (ya regando o fuera de servicio).")
                    continue
                threading.Thread(
                    target=hilo_riego,
                    args=(productor, data.get("duracion_seg", 60), data.get("operario")),
                    daemon=True,
                ).start()

            elif comando == "DETENER_RIEGO":
                if riego_activo.is_set():
                    riego_stop_solicitado.set()

            elif comando == "RESET_CONTADOR":
                global volumen_acumulado
                volumen_acumulado = 0.0
                print(f"[ENGINE {id_ws}] Contador reiniciado.")

            elif comando == "BLOQUEAR":
                with lock_estado:
                    fuera_de_servicio = True
                if riego_activo.is_set():
                    riego_stop_solicitado.set()
                print(f"[ENGINE {id_ws}] >>> FUERA DE SERVICIO por orden de CENTRAL.")

            elif comando == "ACTIVAR":
                with lock_estado:
                    fuera_de_servicio = False
                print(f"[ENGINE {id_ws}] >>> ACTIVADA de nuevo por orden de CENTRAL.")
        time.sleep(0.2)


if __name__ == "__main__":
    ip_monitor = sys.argv[1] if len(sys.argv) > 1 else os.getenv("MONITOR_HOST", "127.0.0.1")
    puerto_monitor = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("MONITOR_PORT", 6001))
    kafka_broker = sys.argv[3] if len(sys.argv) > 3 else os.getenv("KAFKA_BROKER", "localhost:9093")

    identidad_lista = threading.Event()

    threading.Thread(target=hilo_monitor, args=(ip_monitor, puerto_monitor, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_comandos, args=(kafka_broker, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_teclado, daemon=True).start()

    print("[ENGINE] Iniciado. Esperando identidad del Monitor...")
    while True:
        time.sleep(1)
