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

Hilos del Engine:
  hilo_monitor    : cliente TCP del Monitor. Recibe IDENTIDAD y contesta OK/KO a cada PING.
                    Si pierde la conexión, reintenta cada 3 s (bucle de reconexión).
  hilo_comandos   : consumidor Kafka del topic 'comandos-estacion'. Filtra por id_ws/ALL.
                    Gestiona INICIAR_RIEGO, DETENER_RIEGO, RESET_CONTADOR, BLOQUEAR, ACTIVAR.
  hilo_teclado    : lee sys.stdin en bucle. 'k' = simular fuga (KO), 'o' = resolverla (OK),
                    'q' = salir limpiamente. Permite pruebas manuales sin necesidad de red.
  hilo_auto_riego : (opcional) si AUTO_RIEGO_INTERVALO > 0, inicia riegos automáticos cada
                    N segundos sin necesitar operario ni orden de CENTRAL. La estación se
                    riega sola. Se puede desactivar con AUTO_RIEGO_INTERVALO=0 (defecto).
  hilo_riego      : (uno por riego) manda telemetría cada 1 s a 'telemetria-riego' y termina
                    con un mensaje RIEGO_FINALIZADO indicando el motivo de parada.

Motivos de fin de riego: TIEMPO_AGOTADO, FUGA, BLOQUEO, MANUAL.
Sincronización: threading.Event (banderas entre hilos) y Lock (proteger variables compartidas).

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
# El Engine es la "maquina" de la estacion (simula la electrovalvula). Tiene 4 hilos
# fijos y crea uno mas cada vez que hay que regar:
#
#   hilo_monitor    : cliente TCP del Monitor. Recibe IDENTIDAD y contesta OK/KO a cada PING.
#   hilo_comandos   : consumidor Kafka de 'comandos-estacion' (INICIAR_RIEGO, DETENER_RIEGO,
#                     BLOQUEAR, ACTIVAR...). Ignora los comandos dirigidos a otra estacion.
#   hilo_teclado    : lee la consola: 'k' = simular fuga, 'o' = resolverla, 'q' = salir.
#   hilo_auto_riego : (opcional) si AUTO_RIEGO_INTERVALO > 0, inicia riegos automaticos
#                     cada N segundos sin necesitar operario ni CENTRAL. La estacion se
#                     riega sola. Se puede desactivar con AUTO_RIEGO_INTERVALO=0.
#   hilo_riego      : (uno por riego) manda telemetria cada 1 s a 'telemetria-riego' y termina
#                     con un mensaje RIEGO_FINALIZADO indicando el motivo.
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

# Booleano de estado: True cuando la fuga está activa (simulada con 'k').
# Hace que hilo_monitor conteste KO al siguiente PING del Monitor.
# El Monitor detecta KO y publica FUGA en Kafka hacia CENTRAL.
en_fuga = False

# Booleano de estado: True cuando CENTRAL ha enviado el comando BLOQUEAR.
# Impide iniciar nuevos riegos y detiene el riego en curso si lo hubiera.
fuera_de_servicio = False

# Lock que protege en_fuga y fuera_de_servicio: varios hilos los leen y escriben.
lock_estado = threading.Lock()   # protege los dos booleanos anteriores

# threading.Event de sincronización: se activa (set) cuando hay un riego en curso
# y se desactiva (clear) cuando termina. Otros hilos pueden hacer .is_set() para
# saber si hay riego sin necesitar un lock.
riego_activo = threading.Event()

# threading.Event de sincronización: hilo_comandos lo activa cuando recibe DETENER_RIEGO
# o cuando se ejecuta BLOQUEAR. hilo_riego lo comprueba cada segundo y, si está activo,
# para el riego con motivo "MANUAL" o "BLOQUEO" respectivamente.
riego_stop_solicitado = threading.Event()

volumen_acumulado = 0.0
operario_actual = None


# ---------------------------------------------------- enlace con Monitor --
def hilo_monitor(ip_monitor, puerto_monitor, identidad_lista: threading.Event):
    """
    Se conecta al Monitor como cliente TCP y gestiona el handshake y los latidos.

    Flujo:
      1. Bucle de reconexión: si el Monitor no está disponible, reintenta cada 3 s.
         Esto permite arrancar el Engine antes que el Monitor; se conectará en cuanto
         el Monitor esté listo.
      2. Handshake IDENTIDAD: al conectar, el Monitor envía la trama
         IDENTIDAD#id_ws#ubicacion. El Engine la parsea y guarda id_ws y ubicacion
         en las variables globales. Después activa el Event identidad_lista para
         que hilo_comandos y hilo_auto_riego puedan empezar a trabajar.
      3. Bucle de latidos PING/OK/KO: el Monitor envía tramas PING cada
         HEARTBEAT_INTERVALO segundos. El Engine responde:
           OK → todo normal (en_fuga es False).
           KO → fuga activa (en_fuga es True, pulsada 'k' en consola).
         Si no llega PING en 15 s (timeout), se asume que el Monitor cayó y
         el bucle de reconexión vuelve a intentar conectar.
    """
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
    """
    Lee comandos de la consola para simular averías o salir del Engine.

    Teclas disponibles:
      k + Enter → activa en_fuga=True: el Engine empieza a responder KO al PING.
                  El Monitor detecta KO y notifica FUGA a CENTRAL.
      o + Enter → desactiva en_fuga=False: el Engine vuelve a responder OK.
                  El Monitor detecta el cambio y notifica FUGA_RESUELTA a CENTRAL.
      q + Enter → termina el proceso completamente.

    Usa sys.stdin en un bucle bloqueante (espera línea a línea), lo que permite
    leer la entrada estándar sin consumir CPU (el OS notifica cuando hay datos).
    Es la forma más sencilla de teclado en consola para un proceso de simulación.
    """
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
    """
    Simula el ciclo completo de un riego: apertura de válvula, telemetría y cierre.

    Funcionamiento:
      - Cada segundo genera un caudal aleatorio (10-25 L/min) y acumula el volumen.
      - Fórmula del volumen: el caudal está en litros/minuto, pero se mide cada 1 segundo.
        Por eso se divide entre 60: volumen_delta = caudal_lmin / 60 litros/segundo × 1 s.
      - Envía un mensaje TELEMETRIA a Kafka cada segundo con el caudal y el volumen total.
      - Al finalizar, envía un mensaje RIEGO_FINALIZADO con el resumen completo.

    Condiciones de parada (orden de prioridad):
      1. FUGA activa (en_fuga=True): el sistema de seguridad prevalece sobre todo.
      2. BLOQUEO (fuera_de_servicio=True): orden de CENTRAL.
      3. Parada manual (riego_stop_solicitado activo): orden de DETENER_RIEGO.
      4. Tiempo agotado: se cumplió la duración solicitada.

    El orden importa: una fuga durante un riego bloqueado reportará "FUGA", no "BLOQUEO".
    """
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
        # Ejemplo: 18 L/min -> 18/60 = 0.3 L en ese segundo.
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


def hilo_auto_riego(productor, intervalo: int, duracion: int, identidad_lista: threading.Event):
    """
    Inicia riegos automáticos periódicos sin intervención del operario ni de CENTRAL.

    Esta función se activa solo cuando AUTO_RIEGO_INTERVALO > 0 (variable de entorno).
    Permite simular una estación de riego autónoma que se riega sola según un horario.

    Comportamiento:
      - Espera a que hilo_monitor haya recibido la identidad (id_ws) del Monitor antes
        de actuar: sin id_ws no se puede etiquetar la telemetría correctamente.
      - Cada 'intervalo' segundos comprueba si las condiciones permiten regar.
      - Condiciones para POSPONER el auto-riego (no se cancela, se reintenta en el
        próximo ciclo):
          * Hay una fuga activa (en_fuga=True): regar con fuga sería contraproducente.
          * La estación está fuera de servicio (fuera_de_servicio=True): CENTRAL lo prohíbe.
          * Ya hay un riego en curso (riego_activo.is_set()): no se superponen riegos.
      - Si todo está bien, lanza hilo_riego en un hilo daemon con operario='ESTACION'.

    :param productor:      KafkaProducer para publicar la telemetría.
    :param intervalo:      Segundos entre auto-riegos (de AUTO_RIEGO_INTERVALO).
    :param duracion:       Duración de cada auto-riego en segundos (de AUTO_RIEGO_DURACION).
    :param identidad_lista:Event que se activa cuando se conoce el id_ws del Engine.
    """
    identidad_lista.wait()   # esperar a conocer id_ws antes de actuar
    print(f"[ENGINE {id_ws}] Auto-riego activado: cada {intervalo}s durante {duracion}s.")
    while True:
        time.sleep(intervalo)
        # Comprobar condiciones antes de lanzar: sin fuga, sin bloqueo, sin riego activo.
        with lock_estado:
            fuga_actual = en_fuga
            bloqueada   = fuera_de_servicio
        if fuga_actual:
            print(f"[ENGINE {id_ws}] Auto-riego pospuesto: hay una fuga activa.")
            continue
        if bloqueada:
            print(f"[ENGINE {id_ws}] Auto-riego pospuesto: estacion fuera de servicio.")
            continue
        if riego_activo.is_set():
            print(f"[ENGINE {id_ws}] Auto-riego pospuesto: ya hay un riego en curso.")
            continue
        # Todo correcto: lanzar el riego autonomo en su propio hilo.
        print(f"[ENGINE {id_ws}] >>> Auto-riego iniciado ({duracion}s). Operario: ESTACION.")
        threading.Thread(
            target=hilo_riego,
            args=(productor, duracion, "ESTACION"),   # "ESTACION" = auto-gestionado, sin FO
            daemon=True,
        ).start()


def hilo_comandos(kafka_broker, identidad_lista: threading.Event):
    """
    Escucha los comandos que envia CENTRAL (iniciar/detener riego, bloquear, activar...).

    Filtro por id_ws/ALL:
    El topic 'comandos-estacion' es compartido por TODOS los Engines del sistema.
    Cada Engine filtra los mensajes que le corresponden comparando el campo id_ws
    del mensaje con su propio id_ws. Los mensajes con id_ws='ALL' se atienden siempre
    (permiten enviar una orden a todas las estaciones a la vez, aunque no se usa
    actualmente en la práctica).
    """
    global fuera_de_servicio
    identidad_lista.wait()  # necesitamos conocer nuestro id_ws antes de filtrar comandos

    productor = crear_productor(kafka_broker)
    # group_id fijo por estacion: Kafka recuerda hasta que comando se leyo.
    consumidor = crear_consumidor(TOPIC_COMANDOS, kafka_broker, group_id=f"engine-{id_ws}")
    print(f"[ENGINE {id_ws}] Escuchando comandos en '{TOPIC_COMANDOS}'...")

    while True:
        try:
            for msg in consumidor:
                try:
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
                        # Validar duracion.
                        try:
                            duracion = int(data.get("duracion_seg", 60))
                            if duracion <= 0:
                                raise ValueError
                        except (TypeError, ValueError):
                            print(f"[ENGINE {id_ws}][WARN] duracion_seg invalida, usando 60s.")
                            duracion = 60
                        # El riego dura segundos: va en su propio hilo para no bloquear la lectura de comandos.
                        threading.Thread(
                            target=hilo_riego,
                            args=(productor, duracion, data.get("operario")),
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

                    elif comando is not None:
                        print(f"[ENGINE {id_ws}][WARN] Comando desconocido: {comando!r}")

                except Exception as e:
                    print(f"[ENGINE {id_ws}][ERROR] hilo_comandos procesando mensaje: {e}")
        except Exception as e:
            print(f"[ENGINE {id_ws if id_ws else '?'}][ERROR] hilo_comandos loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


if __name__ == "__main__":
    # Parametros: argumento de linea de comandos -> variable de entorno -> valor por defecto.
    ip_monitor     = sys.argv[1] if len(sys.argv) > 1 else os.getenv("MONITOR_HOST", "127.0.0.1")
    puerto_monitor = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("MONITOR_PORT", 6001))
    kafka_broker   = sys.argv[3] if len(sys.argv) > 3 else os.getenv("KAFKA_BROKER", "localhost:9093")

    # Auto-riego autonomo de la estacion (sin operario ni CENTRAL):
    #   AUTO_RIEGO_INTERVALO: segundos entre riegos automaticos (0 = desactivado, defecto: 0).
    #     Aumentar este valor reduce la frecuencia de riego.
    #     Si se pone a 0, el hilo hilo_auto_riego no se crea.
    #   AUTO_RIEGO_DURACION : duracion de cada auto-riego en segundos (defecto: 30).
    #     Debe ser menor que AUTO_RIEGO_INTERVALO para que no se solapen riegos consecutivos.
    auto_intervalo = int(os.getenv("AUTO_RIEGO_INTERVALO", "0"))
    auto_duracion  = int(os.getenv("AUTO_RIEGO_DURACION",  "30"))

    # Event compartido: hilo_monitor lo activa al recibir la identidad;
    # hilo_comandos y hilo_auto_riego esperan a que se active.
    identidad_lista = threading.Event()

    # Productor Kafka compartido entre hilo_auto_riego y hilo_riego.
    # hilo_comandos crea el suyo propio (necesita el id_ws para el group_id).
    productor_auto = None
    if auto_intervalo > 0:
        productor_auto = crear_productor(kafka_broker)

    threading.Thread(target=hilo_monitor,   args=(ip_monitor, puerto_monitor, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_comandos,  args=(kafka_broker, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_teclado,   daemon=True).start()

    if auto_intervalo > 0:
        threading.Thread(
            target=hilo_auto_riego,
            args=(productor_auto, auto_intervalo, auto_duracion, identidad_lista),
            daemon=True,
        ).start()
        print(f"[ENGINE] Auto-riego configurado: cada {auto_intervalo}s durante {auto_duracion}s.")
    else:
        print("[ENGINE] Auto-riego desactivado (AUTO_RIEGO_INTERVALO=0).")

    print("[ENGINE] Iniciado. Esperando identidad del Monitor...")
    # El hilo principal solo mantiene vivo el programa (los demas son daemon).
    while True:
        time.sleep(1)
