# WM_FO/WM_FO.py
"""
WM_FO - Aplicacion del operario de campo (Field Operator).

Permite al operario:
 - Consultar el estado de todas las estaciones de riego (WS) disponibles.
 - Solicitar la activacion manual del riego de una WS concreta.
 - Cargar un fichero JSON con una lista de activaciones a solicitar de
   forma automatica y secuencial (esperando 4s entre cada una, tal y
   como indica el enunciado de la practica).

Uso:
    python WM_FO.py <kafka_broker> <id_operador> [ruta_fichero_activaciones]
"""
import os
import sys
import json
import time
import uuid
import threading

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

from utils.kafka_utils import (
    crear_productor, crear_consumidor,
    TOPIC_PETICIONES, TOPIC_RESPUESTAS, TOPIC_BROADCAST,
)

estaciones_conocidas = {}          # id_ws -> dict con su ultimo estado conocido
lock_estaciones = threading.Lock()

resultados_pendientes = {}         # request_id -> {"event":..., "resultado": None}
lock_resultados = threading.Lock()


def hilo_broadcast(kafka_broker):
    """Mantiene actualizado el listado de todas las WS (disponibles o no)."""
    consumidor = crear_consumidor(TOPIC_BROADCAST, kafka_broker, group_id=None)
    while True:
        for msg in consumidor:
            with lock_estaciones:
                for e in msg.value.get("estaciones", []):
                    estaciones_conocidas[e["id_ws"]] = e
        time.sleep(0.2)


def hilo_respuestas(kafka_broker, id_operador):
    """Escucha las respuestas de CENTRAL dirigidas a este operario."""
    consumidor = crear_consumidor(TOPIC_RESPUESTAS, kafka_broker, group_id=f"fo-{id_operador}-{uuid.uuid4()}")
    while True:
        for msg in consumidor:
            data = msg.value
            if data.get("id_operador") != id_operador:
                continue

            evento = data.get("evento")
            id_ws = data.get("id_ws")

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
            if pendiente and evento in ("AUTORIZADO", "DENEGADO", "RESUMEN", "INTERRUMPIDO"):
                # Solo las respuestas "terminales" liberan la espera del modo fichero
                if evento in ("DENEGADO", "RESUMEN", "INTERRUMPIDO"):
                    pendiente["resultado"] = data
                    pendiente["event"].set()
        time.sleep(0.2)


def solicitar_riego(productor, id_operador, id_ws, duracion_seg, esperar_resultado=False, timeout=None):
    request_id = str(uuid.uuid4())
    evento_resultado = threading.Event()
    with lock_resultados:
        resultados_pendientes[request_id] = {"event": evento_resultado, "resultado": None}

    productor.send(TOPIC_PETICIONES, key=id_operador, value={
        "tipo": "SOLICITUD_RIEGO", "request_id": request_id, "id_operador": id_operador,
        "id_ws": id_ws, "duracion_seg": duracion_seg, "timestamp": time.time(),
    })
    productor.flush()
    print(f"[FO {id_operador}] Peticion enviada: activar {id_ws} durante {duracion_seg}s.")

    if esperar_resultado:
        evento_resultado.wait(timeout=timeout)
        with lock_resultados:
            info = resultados_pendientes.pop(request_id, {})
        return info.get("resultado")
    return None


def mostrar_estaciones():
    with lock_estaciones:
        if not estaciones_conocidas:
            print("(Aun no se ha recibido informacion de CENTRAL. Espera unos segundos...)")
            return
        print(f"\n{'ID':<10}{'UBICACION':<25}{'ESTADO':<20}{'CAUDAL':<10}{'VOLUMEN':<10}{'OPERARIO':<10}")
        for e in sorted(estaciones_conocidas.values(), key=lambda x: x["id_ws"]):
            print(f"{e['id_ws']:<10}{(e.get('ubicacion') or '-'):<25}{e['estado']:<20}"
                  f"{e.get('caudal', 0) or 0:<10}{e.get('volumen', 0) or 0:<10}{(e.get('operario') or '-'):<10}")
    print()


def ejecutar_fichero(productor, id_operador, ruta_fichero):
    with open(ruta_fichero, "r", encoding="utf-8") as f:
        activaciones = json.load(f)

    print(f"[FO {id_operador}] Cargadas {len(activaciones)} activaciones desde '{ruta_fichero}'.")
    for i, act in enumerate(activaciones, start=1):
        id_ws = act["id_ws"]
        duracion = act.get("duracion_seg", 60)
        print(f"\n[FO {id_operador}] ({i}/{len(activaciones)}) Solicitando activacion de {id_ws}...")
        resultado = solicitar_riego(productor, id_operador, id_ws, duracion,
                                     esperar_resultado=True, timeout=duracion + 30)
        if resultado is None:
            print(f"[FO {id_operador}] Sin respuesta de CENTRAL para {id_ws} (timeout).")
        print(f"[FO {id_operador}] Esperando 4s antes de la siguiente peticion...")
        time.sleep(4)
    print(f"[FO {id_operador}] Fichero de activaciones completado.")


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
            try:
                duracion = int(input("Duracion del riego en segundos [60]: ").strip() or "60")
            except ValueError:
                duracion = 60
            solicitar_riego(productor, id_operador, id_ws, duracion)
        elif opcion == "3":
            ruta = input(f"Ruta del fichero JSON [{ruta_fichero_defecto or 'activaciones.json'}]: ").strip()
            ruta = ruta or ruta_fichero_defecto or "activaciones.json"
            try:
                ejecutar_fichero(productor, id_operador, ruta)
            except FileNotFoundError:
                print(f"No se encontro el fichero '{ruta}'.")
        elif opcion == "4":
            print("Hasta luego.")
            os._exit(0)
        else:
            print("Opcion no valida.")


if __name__ == "__main__":
    kafka_broker = sys.argv[1] if len(sys.argv) > 1 else os.getenv("KAFKA_BROKER", "localhost:9093")
    id_operador = sys.argv[2] if len(sys.argv) > 2 else os.getenv("OPERADOR_ID", "FO-01")
    ruta_fichero = sys.argv[3] if len(sys.argv) > 3 else os.getenv("ACTIVACIONES_FILE")

    print(f"[FO {id_operador}] Conectando a Kafka ({kafka_broker})...")
    productor = crear_productor(kafka_broker)

    threading.Thread(target=hilo_broadcast, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_respuestas, args=(kafka_broker, id_operador), daemon=True).start()

    time.sleep(1)  # pequeno margen para que los consumidores arranquen
    menu(productor, id_operador, ruta_fichero)
