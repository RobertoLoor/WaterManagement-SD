# WM_WS/WM_WS_E.py
"""
Engine de estación: simula la electroválvula física y el sensor de caudal.
Recibe su identidad y latidos del Monitor local por sockets TCP, escucha comandos
de riego desde Central por Kafka y emite telemetría periódica segundo a segundo.
"""
import os
import sys
import socket
import threading
import time
import random

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

from utils.protocol import empaquetar_trama, desempaquetar_trama, recv_exact_frame
from utils.kafka_utils import crear_productor, crear_consumidor, TOPIC_TELEMETRIA, TOPIC_COMANDOS

# Identidad de la estación: la recibe del Monitor al conectarse
id_ws = None
ubicacion = None

en_fuga = False
fuera_de_servicio = False

# Lock para proteger variables compartidas entre los hilos de teclado, monitor, comandos y riego
lock_estado = threading.Lock()
# Eventos: banderas booleanas seguras para avisar entre hilos sin condiciones de carrera
riego_activo = threading.Event()          # True mientras la válvula está abierta y regando
riego_stop_solicitado = threading.Event() # True si se pide parar el riego antes de tiempo

volumen_acumulado = 0.0
operario_actual = None


# Conecta con el Monitor local por socket TCP, recibe la identidad y responde a los latidos PING.
def hilo_monitor(ip_monitor, puerto_monitor, identidad_lista: threading.Event):
    global id_ws, ubicacion
    while True:
        sock = None
        try:
            # Conexión TCP con el Monitor de su misma estación
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip_monitor, puerto_monitor))
            print(f"[ENGINE] Conectado al Monitor en {ip_monitor}:{puerto_monitor}")

            # Espera la trama inicial IDENTIDAD#id#ubicacion que envía el Monitor
            trama = recv_exact_frame(sock, timeout=10)
            if not trama:
                raise ConnectionError("No se recibio respuesta de identidad del Monitor.")

            valido, mensaje = desempaquetar_trama(trama)
            if not valido or not mensaje.startswith("IDENTIDAD#"):
                raise ConnectionError(f"Trama de identidad invalida recibida: {mensaje!r}")

            partes = mensaje.split('#')
            if len(partes) < 3:
                raise ConnectionError(f"Formato de identidad incompleto: {mensaje!r}")

            id_ws, ubicacion = partes[1], partes[2]
            print(f"[ENGINE] Identidad recibida: {id_ws} @ {ubicacion}")
            # .set(): activa el evento para desbloquear a los hilos de comandos y auto-riego que esperan la identidad.
            identidad_lista.set()

            # Bucle de latidos: responde a cada PING del Monitor con OK o KO según el estado de avería
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
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass


# Lee entradas del teclado en segundo plano para permitir simular averías interactivamente.
def hilo_teclado():
    global en_fuga
    # Comprueba que la consola soporte entrada interactiva (evita errores en entornos como Docker)
    if not sys.stdin or not hasattr(sys.stdin, "readline"):
        return

    print("[ENGINE] Pulsa 'k' + Enter para simular una fuga, 'o' + Enter para resolverla.")
    try:
        # Lee línea a línea de la entrada estándar de la consola (sys.stdin)
        for linea in sys.stdin:
            if not linea:
                break
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
                # os._exit(0): termina el proceso de forma inmediata sin esperar a los demás hilos.
                os._exit(0)
    except (KeyboardInterrupt, EOFError):
        pass
    except Exception as e:
        print(f"[ENGINE][WARN] hilo_teclado finalizado: {e}")


# Simula el ciclo de riego abriendo la válvula, emitiendo telemetría y acumulando volumen.
def hilo_riego(productor, duracion_seg: int, operario: str):
    global volumen_acumulado, operario_actual
    with lock_estado:
        volumen_acumulado = 0.0
        operario_actual = operario
        # Limpia cualquier petición de parada de riegos anteriores
        riego_stop_solicitado.clear()

    inicio = time.time()
    motivo_fin = "TIEMPO_AGOTADO"
    print(f"[ENGINE {id_ws}] >>> Electrovalvula ABIERTA. Riego durante {duracion_seg}s (operario: {operario}).")

    try:
        while True:
            with lock_estado:
                fuga_actual = en_fuga
                bloqueada = fuera_de_servicio

            # Comprueba las 4 condiciones de parada en orden de prioridad: FUGA > BLOQUEO > MANUAL > TIEMPO
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

            # Genera un caudal aleatorio simulado entre 10.0 y 25.0 litros por minuto
            caudal = round(random.uniform(10.0, 25.0), 2)
            with lock_estado:
                # caudal / 60.0: convierte de litros/minuto a los litros consumidos en este segundo concreto.
                volumen_acumulado = round(volumen_acumulado + caudal / 60.0, 2)
                vol_actual = volumen_acumulado

            try:
                # Envía lectura periódica de telemetría a Central mediante Kafka
                productor.send(TOPIC_TELEMETRIA, key=id_ws, value={
                    "tipo": "TELEMETRIA", "id_ws": id_ws, "caudal_lmin": caudal,
                    "volumen_total_l": vol_actual, "timestamp": time.time(),
                })
            except Exception as e_send:
                print(f"[ENGINE {id_ws}][ERROR] Error enviando telemetria: {e_send}")

            print(f"[ENGINE {id_ws}] Regando... caudal={caudal} L/min | acumulado={vol_actual} L")
            time.sleep(1)

        duracion_real = round(time.time() - inicio, 1)
        with lock_estado:
            vol_final = volumen_acumulado
        print(f"[ENGINE {id_ws}] >>> Electrovalvula CERRADA. Motivo: {motivo_fin}. Volumen total: {vol_final} L.")

        try:
            # Envía el mensaje final RIEGO_FINALIZADO con el balance del riego
            productor.send(TOPIC_TELEMETRIA, key=id_ws, value={
                "tipo": "RIEGO_FINALIZADO", "id_ws": id_ws, "operario": operario,
                "motivo": motivo_fin, "volumen_total_l": vol_final,
                "duracion_seg": duracion_real, "timestamp": time.time(),
            })
            productor.flush()
        except Exception as e_flush:
            print(f"[ENGINE {id_ws}][ERROR] Error enviando RIEGO_FINALIZADO: {e_flush}")

    except Exception as e:
        print(f"[ENGINE {id_ws}][ERROR] Excepcion inesperada en hilo_riego: {e}")
    finally:
        # El bloque finally se ejecuta SIEMPRE para asegurar que la estación quede libre aunque haya error.
        riego_activo.clear()
        with lock_estado:
            operario_actual = None


# Temporizador que inicia riegos periódicos automáticos si la estación está disponible.
def hilo_auto_riego(kafka_broker: str, intervalo: int, duracion: int, identidad_lista: threading.Event):
    # .wait() pausa este hilo hasta que el hilo_monitor reciba la identidad del Monitor
    identidad_lista.wait()
    productor = crear_productor(kafka_broker)
    print(f"[ENGINE {id_ws}] Auto-riego activado: cada {intervalo}s durante {duracion}s.")

    while True:
        time.sleep(intervalo)
        with lock_estado:
            if en_fuga:
                print(f"[ENGINE {id_ws}] Auto-riego pospuesto: hay una fuga activa.")
                continue
            if fuera_de_servicio:
                print(f"[ENGINE {id_ws}] Auto-riego pospuesto: estacion fuera de servicio.")
                continue
            if riego_activo.is_set():
                print(f"[ENGINE {id_ws}] Auto-riego pospuesto: ya hay un riego en curso.")
                continue
            # Marcamos atómicamente que el riego está activo antes de crear el hilo para evitar colisiones
            riego_activo.set()

        print(f"[ENGINE {id_ws}] >>> Auto-riego iniciado ({duracion}s). Operario: ESTACION.")
        threading.Thread(
            target=hilo_riego,
            args=(productor, duracion, "ESTACION"),
            daemon=True,
        ).start()


# Escucha comandos de Central por Kafka y ejecuta órdenes de riego o cambio de estado.
def hilo_comandos(kafka_broker, identidad_lista: threading.Event):
    global fuera_de_servicio, volumen_acumulado
    identidad_lista.wait()

    productor = crear_productor(kafka_broker)
    # Crea un consumidor con group_id exclusivo para este Engine para recibir sus órdenes
    consumidor = crear_consumidor(TOPIC_COMANDOS, kafka_broker, group_id=f"engine-{id_ws}")
    print(f"[ENGINE {id_ws}] Escuchando comandos en '{TOPIC_COMANDOS}'...")

    while True:
        try:
            for msg in consumidor:
                try:
                    data = msg.value
                    destino = data.get("id_ws")
                    # Filtra solo los mensajes dirigidos a esta estación concreta o a todas ("ALL")
                    if destino not in (id_ws, "ALL"):
                        continue

                    comando = data.get("comando")

                    if comando == "INICIAR_RIEGO":
                        with lock_estado:
                            bloqueada = fuera_de_servicio
                            if riego_activo.is_set() or bloqueada:
                                print(f"[ENGINE {id_ws}] Orden INICIAR_RIEGO ignorada (ya regando o fuera de servicio).")
                                continue
                            riego_activo.set()

                        try:
                            duracion = int(data.get("duracion_seg", 60))
                            if duracion <= 0:
                                raise ValueError
                        except (TypeError, ValueError):
                            duracion = 60

                        # Lanza el riego en un hilo aparte para no bloquear este bucle de comandos de Kafka
                        threading.Thread(
                            target=hilo_riego,
                            args=(productor, duracion, data.get("operario")),
                            daemon=True,
                        ).start()

                    elif comando == "DETENER_RIEGO":
                        if riego_activo.is_set():
                            # Activa la señal para que hilo_riego termine limpiamente en el próximo segundo
                            riego_stop_solicitado.set()

                    elif comando == "RESET_CONTADOR":
                        with lock_estado:
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

                except Exception as e:
                    print(f"[ENGINE {id_ws}][ERROR] hilo_comandos procesando mensaje: {e}")
        except Exception as e:
            print(f"[ENGINE {id_ws if id_ws else '?'}][ERROR] hilo_comandos loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


if __name__ == "__main__":
    ip_monitor = sys.argv[1] if len(sys.argv) > 1 else os.getenv("MONITOR_HOST", "127.0.0.1")

    try:
        puerto_monitor = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.getenv("MONITOR_PORT", "6001"))
    except ValueError:
        puerto_monitor = 6001

    kafka_broker = sys.argv[3] if len(sys.argv) > 3 else os.getenv("KAFKA_BROKER", "localhost:9093")

    try:
        auto_intervalo = int(os.getenv("AUTO_RIEGO_INTERVALO", "0"))
        if auto_intervalo < 0:
            auto_intervalo = 0
    except ValueError:
        auto_intervalo = 0

    try:
        auto_duracion = int(os.getenv("AUTO_RIEGO_DURACION", "30"))
        if auto_duracion <= 0:
            auto_duracion = 30
    except ValueError:
        auto_duracion = 30

    # Evento de sincronización: se activará cuando el Monitor envíe la identidad a este Engine
    identidad_lista = threading.Event()

    threading.Thread(target=hilo_monitor,   args=(ip_monitor, puerto_monitor, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_comandos,  args=(kafka_broker, identidad_lista), daemon=True).start()
    threading.Thread(target=hilo_teclado,   daemon=True).start()

    if auto_intervalo > 0:
        threading.Thread(
            target=hilo_auto_riego,
            args=(kafka_broker, auto_intervalo, auto_duracion, identidad_lista),
            daemon=True,
        ).start()
        print(f"[ENGINE] Auto-riego configurado: cada {auto_intervalo}s durante {auto_duracion}s.")
    else:
        print("[ENGINE] Auto-riego desactivado (AUTO_RIEGO_INTERVALO=0).")

    print("[ENGINE] Iniciado. Esperando identidad del Monitor...")
    try:
        # Bucle principal que mantiene el proceso vivo durmiendo 1 segundo cada vez
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[ENGINE] Deteniendo por señal de teclado (Ctrl+C)...")
        sys.exit(0)
