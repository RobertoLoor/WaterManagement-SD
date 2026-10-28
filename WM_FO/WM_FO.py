# WM_FO/WM_FO.py
"""
Interfaz de consola para el operario de campo (Field Operator).
Se comunica exclusivamente mediante Kafka para supervisar el estado de las estaciones,
solicitar activaciones manuales de riego o ejecutar lotes desde un fichero JSON.
"""
import os
import sys
import json
import time
import uuid
import threading

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

from utils.kafka_utils import (
    crear_productor, crear_consumidor,
    TOPIC_PETICIONES, TOPIC_RESPUESTAS, TOPIC_BROADCAST,
)

# Diccionario en memoria donde se guardan las estaciones recibidas por broadcast
estaciones_conocidas = {}
lock_estaciones = threading.Lock()

# Diccionario para guardar eventos de espera de respuestas pendientes por request_id
resultados_pendientes = {}
lock_resultados = threading.Lock()


# Hilo que escucha el estado global de la red emitido por Central y actualiza la lista local.
def hilo_broadcast(kafka_broker):
    # group_id=None: no guarda posición de lectura en Kafka, solo recibe mensajes nuevos en tiempo real
    consumidor = crear_consumidor(TOPIC_BROADCAST, kafka_broker, group_id=None)
    while True:
        try:
            for msg in consumidor:
                try:
                    # Actualiza la lista de estaciones en memoria con los datos recibidos
                    with lock_estaciones:
                        for e in msg.value.get("estaciones", []):
                            if "id_ws" in e:
                                estaciones_conocidas[e["id_ws"]] = e
                except Exception as e_inner:
                    print(f"[FO][ERROR] Procesando broadcast: {e_inner}")
        except Exception as e:
            print(f"[FO][ERROR] hilo_broadcast: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


# Hilo que escucha las respuestas y resúmenes de riego enviados por Central a este operario.
def hilo_respuestas(kafka_broker, id_operador):
    # group_id con UUID y desde_inicio=True: si el operario arranca tarde, lee el histórico y no pierde respuestas
    consumidor = crear_consumidor(TOPIC_RESPUESTAS, kafka_broker, group_id=f"fo-{id_operador}-{uuid.uuid4()}", desde_inicio=True)
    while True:
        try:
            for msg in consumidor:
                try:
                    data = msg.value
                    # Filtra solo los mensajes dirigidos a este operario concreto
                    if data.get("id_operador") != id_operador:
                        continue

                    evento = data.get("evento")
                    id_ws = data.get("id_ws", "?")

                    if evento == "AUTORIZADO":
                        print(f"\n[CENTRAL] Riego AUTORIZADO en {id_ws}. {data.get('motivo', '')}")
                    elif evento == "DENEGADO":
                        print(f"\n[CENTRAL] Riego DENEGADO en {id_ws}. Motivo: {data.get('motivo', '')}")
                    elif evento == "INTERRUMPIDO":
                        print(f"\n[CENTRAL] Riego INTERRUMPIDO en {id_ws}. Motivo: {data.get('motivo', '')}")
                    elif evento == "RESUMEN":
                        print(f"\n[CENTRAL] Riego finalizado en {id_ws} ({data.get('motivo')}). "
                              f"Volumen total: {data.get('volumen_total_l', 0)} L, "
                              f"duracion: {data.get('duracion_seg', 0)} s.")

                    request_id = data.get("request_id")
                    with lock_resultados:
                        pendiente = resultados_pendientes.get(request_id)
                    # Si el modo fichero estaba esperando esta respuesta, solo los eventos de fin desbloquean la espera
                    if pendiente and evento in ("AUTORIZADO", "DENEGADO", "RESUMEN", "INTERRUMPIDO"):
                        # AUTORIZADO solo avisa de que empezó: se espera hasta que termine con DENEGADO, RESUMEN o INTERRUMPIDO
                        if evento in ("DENEGADO", "RESUMEN", "INTERRUMPIDO"):
                            pendiente["resultado"] = data
                            # .set(): despierta al hilo que estaba bloqueado esperando en solicitar_riego()
                            pendiente["event"].set()
                except Exception as e:
                    print(f"[FO {id_operador}][ERROR] Procesando respuesta: {e}")
        except Exception as e:
            print(f"[FO {id_operador}][ERROR] hilo_respuestas: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


# Envía una solicitud de riego a Central por Kafka y opcionalmente espera la respuesta final.
def solicitar_riego(productor, id_operador, id_ws, duracion_seg, esperar_resultado=False, timeout=None):
    if not id_ws:
        print(f"[FO {id_operador}][WARN] El id de la estacion no puede estar vacio.")
        return None

    # Genera un identificador único para emparejar esta petición con su respuesta asíncrona en Kafka
    request_id = str(uuid.uuid4())
    evento_resultado = None
    if esperar_resultado:
        # Crea un Evento de threading que permite pausar la ejecución hasta que llegue la respuesta
        evento_resultado = threading.Event()
        with lock_resultados:
            resultados_pendientes[request_id] = {"event": evento_resultado, "resultado": None}

    try:
        productor.send(TOPIC_PETICIONES, key=id_operador, value={
            "tipo": "SOLICITUD_RIEGO", "request_id": request_id, "id_operador": id_operador,
            "id_ws": id_ws, "duracion_seg": duracion_seg, "timestamp": time.time(),
        })
        productor.flush()
        print(f"[FO {id_operador}] Peticion enviada: activar {id_ws} durante {duracion_seg}s.")
    except Exception as e:
        print(f"[FO {id_operador}][ERROR] No se pudo enviar peticion a Kafka: {e}")
        if esperar_resultado:
            with lock_resultados:
                resultados_pendientes.pop(request_id, None)
        return None

    if esperar_resultado and evento_resultado:
        # Pausa el hilo hasta que hilo_respuestas llame a .set() o venza el timeout
        evento_resultado.wait(timeout=timeout)
        with lock_resultados:
            info = resultados_pendientes.pop(request_id, {})
        return info.get("resultado")
    return None


# Muestra en la consola una tabla con todas las estaciones conocidas y su estado actual.
def mostrar_estaciones():
    with lock_estaciones:
        if not estaciones_conocidas:
            print("(Aun no se ha recibido informacion de CENTRAL. Espera unos segundos...)")
            return
        # Formateo con ancho fijo (<10, <25) para que los datos salgan alineados como una tabla limpia
        print(f"\n{'ID':<10}{'UBICACION':<25}{'ESTADO':<20}{'CAUDAL':<10}{'VOLUMEN':<10}{'OPERARIO':<10}")
        for e in sorted(estaciones_conocidas.values(), key=lambda x: x.get("id_ws", "")):
            print(f"{e.get('id_ws', '?'):<10}{(e.get('ubicacion') or '-'):<25}{e.get('estado', '?'):<20}"
                  f"{e.get('caudal', 0) or 0:<10}{e.get('volumen', 0) or 0:<10}{(e.get('operario') or '-'):<10}")
    print()


# Lee un archivo JSON con peticiones de riego y las ejecuta secuencialmente esperando su resultado.
def ejecutar_fichero(productor, id_operador, ruta_fichero):
    try:
        # Abre el archivo y transforma el contenido JSON en una lista de Python
        with open(ruta_fichero, "r", encoding="utf-8") as f:
            activaciones = json.load(f)
    except FileNotFoundError:
        print(f"[FO {id_operador}][ERROR] No se encontro el fichero '{ruta_fichero}'.")
        return
    except json.JSONDecodeError as e:
        print(f"[FO {id_operador}][ERROR] Fichero JSON mal formado: {e}")
        return
    except Exception as e:
        print(f"[FO {id_operador}][ERROR] Error al leer fichero '{ruta_fichero}': {e}")
        return

    if not isinstance(activaciones, list):
        print(f"[FO {id_operador}][ERROR] El fichero debe contener una lista JSON de activaciones.")
        return

    print(f"[FO {id_operador}] Cargadas {len(activaciones)} activaciones desde '{ruta_fichero}'.")
    for i, act in enumerate(activaciones, start=1):
        id_ws = act.get("id_ws")
        if not id_ws:
            print(f"[FO {id_operador}][WARN] Entrada {i} sin 'id_ws', omitiendo.")
            continue
        duracion = act.get("duracion_seg", 60)
        print(f"\n[FO {id_operador}] ({i}/{len(activaciones)}) Solicitando activacion de {id_ws}...")
        # esperar_resultado=True bloquea hasta que el riego termina antes de pasar al siguiente
        resultado = solicitar_riego(productor, id_operador, id_ws, duracion,
                                     esperar_resultado=True, timeout=duracion + 30)
        if resultado is None:
            print(f"[FO {id_operador}] Sin respuesta de CENTRAL para {id_ws} (timeout).")
        print(f"[FO {id_operador}] Esperando 4s antes de la siguiente peticion...")
        time.sleep(4)
    print(f"[FO {id_operador}] Fichero de activaciones completado.")


# Muestra el menú interactivo por consola y gestiona las opciones seleccionadas por el usuario.
def menu(productor, id_operador, ruta_fichero_defecto=None):
    while True:
        print("\n===== WM_FO - Menu del operario =====")
        print("1) Ver estaciones (disponibles y su estado)")
        print("2) Solicitar activacion manual de riego")
        print("3) Ejecutar activaciones desde fichero")
        print("4) Salir")
        opcion = input("Elige una opcion: ").strip()

        if opcion == "1":
            mostrar_estaciones()
        elif opcion == "2":
            id_ws = input("Id de la estacion (ej. WS-01): ").strip()
            if not id_ws:
                print("El Id de la estacion no puede estar vacio.")
                continue
            try:
                duracion = int(input("Duracion del riego en segundos [60]: ").strip() or "60")
                if duracion <= 0:
                    duracion = 60
            except ValueError:
                duracion = 60
            solicitar_riego(productor, id_operador, id_ws, duracion)
        elif opcion == "3":
            ruta = input(f"Ruta del fichero JSON [{ruta_fichero_defecto or 'activaciones.json'}]: ").strip()
            ruta = ruta or ruta_fichero_defecto or "activaciones.json"
            ejecutar_fichero(productor, id_operador, ruta)
        elif opcion == "4":
            print("Hasta luego.")
            # os._exit(0): cierra inmediatamente el proceso sin esperar al apagado de hilos daemon
            os._exit(0)
        else:
            print("Opcion no valida.")


if __name__ == "__main__":
    kafka_broker = sys.argv[1] if len(sys.argv) > 1 else os.getenv("KAFKA_BROKER", "localhost:9093")
    id_operador = sys.argv[2] if len(sys.argv) > 2 else os.getenv("OPERADOR_ID", "FO-01")
    ruta_fichero = sys.argv[3] if len(sys.argv) > 3 else os.getenv("ACTIVACIONES_FILE")
    modo_auto = (len(sys.argv) > 3) or (os.getenv("AUTO_MODE", "0") == "1")

    print(f"[FO {id_operador}] Conectando a Kafka ({kafka_broker})...")
    productor = crear_productor(kafka_broker)

    # Inicia los hilos de escucha en segundo plano
    threading.Thread(target=hilo_broadcast, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_respuestas, args=(kafka_broker, id_operador), daemon=True).start()

    time.sleep(1)

    # Si se pasó un fichero por parámetro o variable de entorno, corre en modo desatendido
    if modo_auto and ruta_fichero:
        ejecutar_fichero(productor, id_operador, ruta_fichero)
        print(f"[FO {id_operador}] Modo automatico completado. Saliendo.")
        os._exit(0)
    else:
        menu(productor, id_operador, ruta_fichero)
