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

# ---------------------------------------------------------------------------
# VISION GENERAL PARA ESTUDIAR
# ---------------------------------------------------------------------------
# El Engine es la "maquina" de la estacion (simula la electrovalvula). Tiene 3 hilos
# fijos y crea uno mas cada vez que hay que regar:
#
#   hilo_monitor   : cliente TCP del Monitor. Recibe IDENTIDAD y contesta OK/KO a cada PING.
#   hilo_comandos  : consumidor Kafka de 'comandos-estacion' (INICIAR_RIEGO, DETENER_RIEGO,
#                    BLOQUEAR, ACTIVAR...). Ignora los comandos dirigidos a otra estacion.
#   hilo_teclado   : lee la consola: 'k' = simular fuga, 'o' = resolverla, 'q' = salir.
#   hilo_riego     : (uno por riego) manda telemetria cada 1 s a 'telemetria-riego' y termina
#                    con un mensaje RIEGO_FINALIZADO indicando el motivo.
#
# Motivos de fin de riego: TIEMPO_AGOTADO, FUGA, BLOQUEO, MANUAL.
# Sincronizacion: Event (banderas entre hilos) y Lock (proteger variables compartidas).
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

from utils.protocol import empaquetar_trama, desempaquetar_trama, recv_exact_frame
from utils.kafka_utils import crear_productor, crear_consumidor, TOPIC_TELEMETRIA, TOPIC_COMANDOS

# --------------------------------------------------------------- estado ---
# Identidad: la asigna el Monitor al conectar (al principio son None).
id_ws = None
ubicacion = None
# en_fuga: fuga simulada con la tecla 'k' (hace que se conteste KO al PING).
# fuera_de_servicio: bloqueada por CENTRAL (comando BLOQUEAR).
en_fuga = False
fuera_de_servicio = False
lock_estado = threading.Lock()   # protege los dos booleanos anteriores

# riego_activo: bandera "hay un riego en curso". riego_stop_solicitado: orden de parar.
riego_activo = threading.Event()
riego_stop_solicitado = threading.Event()
volumen_acumulado = 0.0
operario_actual = None


# ---------------------------------------------------- enlace con Monitor --
def hilo_monitor(ip_monitor, puerto_monitor, identidad_lista: threading.Event):
    """Se conecta al Monitor, recibe su identidad y responde a los latidos (PING)."""
    global id_ws, ubicacion
    # Bucle externo: si se pierde la conexion, se reintenta cada 3 s (asi ves el
    # "Conexion con Monitor perdida ... Reintentando" mientras el Monitor no esta arrancado).
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip_monitor, puerto_monitor))
            print(f"[ENGINE] Conectado al Monitor en {ip_monitor}:{puerto_monitor}")

            # Lo primero que manda el Monitor es IDENTIDAD#id#ubicacion.
            trama = recv_exact_frame(sock, timeout=10)
            valido, mensaje = desempaquetar_trama(trama) if trama else (False, "")
            if valido and mensaje.startswith("IDENTIDAD#"):
                partes = mensaje.split('#')
                id_ws, ubicacion = partes[1], partes[2]
                print(f"[ENGINE] Identidad recibida: {id_ws} @ {ubicacion}")
                # Avisamos a hilo_comandos de que ya conocemos nuestro id (lo esperaba).
                identidad_lista.set()

            # Bucle de latidos: contestamos a cada PING con OK o KO.
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
    # Recorre las lineas que el usuario escribe en la consola (bloquea esperando Enter).
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
    # Preparacion: contador a 0, marcamos que hay riego en curso y limpiamos la orden de parar.
    volumen_acumulado = 0.0
    operario_actual = operario
    riego_activo.set()
    riego_stop_solicitado.clear()
    inicio = time.time()
    motivo_fin = "TIEMPO_AGOTADO"
    print(f"[ENGINE {id_ws}] >>> Electrovalvula ABIERTA. Riego durante {duracion_seg}s (operario: {operario}).")

    while True:
        # Leemos el estado compartido dentro del lock y trabajamos con copias.
        with lock_estado:
            fuga_actual = en_fuga
            bloqueada = fuera_de_servicio

        # Condiciones de parada, por orden de prioridad.
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

        # Simulacion: caudal aleatorio 10-25 L/min. Como medimos cada segundo, el
        # volumen que se suma en ese segundo es caudal/60 litros.
        caudal = round(random.uniform(10.0, 25.0), 2)
        volumen_acumulado = round(volumen_acumulado + caudal / 60.0, 2)

        # Telemetria a Kafka (CENTRAL la guarda en la BD y la muestra en el dashboard).
        productor.send(TOPIC_TELEMETRIA, key=id_ws, value={
            "tipo": "TELEMETRIA", "id_ws": id_ws, "caudal_lmin": caudal,
            "volumen_total_l": volumen_acumulado, "timestamp": time.time(),
        })
        print(f"[ENGINE {id_ws}] Regando... caudal={caudal} L/min | acumulado={volumen_acumulado} L")
        time.sleep(1)

    # Fin del riego: cerramos la "electrovalvula" y mandamos el resumen a CENTRAL.
    duracion_real = round(time.time() - inicio, 1)
    print(f"[ENGINE {id_ws}] >>> Electrovalvula CERRADA. Motivo: {motivo_fin}. Volumen total: {volumen_acumulado} L.")
    productor.send(TOPIC_TELEMETRIA, key=id_ws, value={
        "tipo": "RIEGO_FINALIZADO", "id_ws": id_ws, "operario": operario_actual,
        "motivo": motivo_fin, "volumen_total_l": volumen_acumulado,
        "duracion_seg": duracion_real, "timestamp": time.time(),
    })
    # flush() fuerza el envio de todo lo pendiente (send() es asincrono).
    productor.flush()
    riego_activo.clear()
    operario_actual = None


def hilo_comandos(kafka_broker, identidad_lista: threading.Event):
    """Escucha los comandos que envia CENTRAL (iniciar/detener riego, bloquear, activar...)."""
    global fuera_de_servicio
    identidad_lista.wait()  # necesitamos conocer nuestro id_ws antes de filtrar comandos

    productor = crear_productor(kafka_broker)
    # group_id fijo por estacion: Kafka recuerda hasta que comando se leyo.
    consumidor = crear_consumidor(TOPIC_COMANDOS, kafka_broker, group_id=f"engine-{id_ws}")
    print(f"[ENGINE {id_ws}] Escuchando comandos en '{TOPIC_COMANDOS}'...")

    while True:
        for msg in consumidor:
            data = msg.value
            destino = data.get("id_ws")
            # El topic lo comparten todas las estaciones: nos quedamos solo con lo nuestro
            # (o con lo dirigido a "ALL", es decir, a todas).
            if destino not in (id_ws, "ALL"):
                continue

            comando = data.get("comando")

            if comando == "INICIAR_RIEGO":
                with lock_estado:
                    bloqueada = fuera_de_servicio
                # No se inicia un segundo riego a la vez ni si esta bloqueada.
                if riego_activo.is_set() or bloqueada:
                    print(f"[ENGINE {id_ws}] Orden INICIAR_RIEGO ignorada (ya regando o fuera de servicio).")
                    continue
                # El riego dura segundos: va en su propio hilo para no bloquear la lectura de comandos.
                threading.Thread(
                    target=hilo_riego,
                    args=(productor, data.get("duracion_seg", 60), data.get("operario")),
                    daemon=True,
                ).start()

            elif comando == "DETENER_RIEGO":
                # No para el hilo a la fuerza: levanta una bandera que hilo_riego comprueba cada ciclo.
                if riego_activo.is_set():
                    riego_stop_solicitado.set()

            elif comando == "RESET_CONTADOR":
                global volumen_acumulado
                volumen_acumulado = 0.0
                print(f"[ENGINE {id_ws}] Contador reiniciado.")

            elif comando == "BLOQUEAR":
                # Pone la estacion fuera de servicio y corta el riego si lo hubiera.
                with lock_estado:
                    fuera_de_servicio = True
                if riego_activo.is_set():
                    riego_stop_solicitado.set()
                print(f"[ENGINE {id_ws}] >>> FUERA DE SERVICIO por orden de CENTRAL.")

            elif comando == "ACTIVAR":
                # Levanta el bloqueo: vuelve a poder regar.
                with lock_estado:
                    fuera_de_servicio = False
                print(f"[ENGINE {id_ws}] >>> ACTIVADA de nuevo por orden de CENTRAL.")
        time.sleep(0.2)


if __name__ == "__main__":
    # Parametros: argumento de linea de comandos -> variable de entorno -> valor por defecto.
    ip_monitor = sys.argv[1] if len(sys.argv) > 1 else os.getenv("MONITOR_HOST", "127.0.0.1")
    puerto_monitor = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("MONITOR_PORT", 6001))
    kafka_broker = sys.argv[3] if len(sys.argv) > 3 else os.getenv("KAFKA_BROKER", "localhost:9093")

    # Event compartido: hilo_monitor lo activa al recibir la identidad; hilo_comandos espera a que se active.
    identidad_lista = threading.Event()

    threading.Thread(target=hilo_monitor, args=(ip_monitor, puerto_monitor, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_comandos, args=(kafka_broker, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_teclado, daemon=True).start()

    print("[ENGINE] Iniciado. Esperando identidad del Monitor...")
    # El hilo principal solo mantiene vivo el programa (los demas son daemon).
    while True:
        time.sleep(1)