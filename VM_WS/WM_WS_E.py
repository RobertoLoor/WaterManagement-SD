# WM_WS/WM_WS_E.py
import os
import sys
import time
import threading
import random

# Buscar dinámicamente la raíz del proyecto (.env + utils/)
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
        raise RuntimeError(
            "No se encontró la raíz del proyecto. Asegúrate de incluir la carpeta 'utils' y el archivo '.env'."
        )

    dir_cursor = parent


from utils.kafka_utils import crear_productor, crear_consumidor

# Estado global del motor de riego
estado_riego = False
volumen_acumulado = 0.0
lock = threading.Lock()


def hilo_telemetria(broker_kafka: str, id_ws: str):
    """Envía telemetría periódica a Kafka mientras el riego esté activo."""
    global estado_riego, volumen_acumulado
    productor = crear_productor(broker_kafka)
    topico_telemetria = "telemetria-riego"

    print(f"[ENGINE {id_ws}] Hilo de telemetría iniciado en tópico '{topico_telemetria}'.")

    while True:
        with lock:
            activo = estado_riego

        if activo:
            # Simular lectura de sensor de caudal (entre 10.0 y 25.0 L/min)
            caudal = round(random.uniform(10.0, 25.0), 2)
            litros_por_segundo = caudal / 60.0

            with lock:
                volumen_acumulado += litros_por_segundo
                vol_actual = round(volumen_acumulado, 2)

            payload = {
                "id_ws": id_ws,
                "caudal_lmin": caudal,
                "volumen_total_l": vol_actual,
                "timestamp": time.time()
            }

            productor.send(topico_telemetria, payload)
            print(f"[TELEMETRÍA {id_ws}] Caudal: {caudal} L/min | Acumulado: {vol_actual} L")

        time.sleep(1)


def escuchar_comandos(broker_kafka: str, id_ws: str):
    """Escucha comandos de control emitidos a través de Kafka."""
    global estado_riego, volumen_acumulado
    topico_comandos = "comandos-estacion"
    consumidor = crear_consumidor(topico_comandos, broker_kafka, group_id=f"engine-{id_ws}")

    print(f"[ENGINE {id_ws}] Escuchando comandos en tópico '{topico_comandos}'...")

    for mensaje in consumidor:
        data = mensaje.value
        target_id = data.get("id_ws")
        comando = data.get("comando")

        # Filtrar si el mensaje es para esta estación o global ("ALL")
        if target_id == id_ws or target_id == "ALL":
            if comando == "INICIAR_RIEGO":
                with lock:
                    estado_riego = True
                print(f"\n[ENGINE {id_ws}] >>> COMANDO RECIBIDO: Riego INICIADO.")
            elif comando == "DETENER_RIEGO":
                with lock:
                    estado_riego = False
                print(f"\n[ENGINE {id_ws}] >>> COMANDO RECIBIDO: Riego DETENIDO.")
            elif comando == "RESET_CONTADOR":
                with lock:
                    volumen_acumulado = 0.0
                print(f"\n[ENGINE {id_ws}] >>> COMANDO RECIBIDO: Contador reiniciado.")


if __name__ == "__main__":
    # Si se ejecuta fuera de Docker, por defecto usa localhost:9093 para Kafka
    broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:9093"
    id_ws = sys.argv[2] if len(sys.argv) > 2 else "WS-01"

    # Lanzar hilo secundario para emisión de telemetría
    hilo = threading.Thread(target=hilo_telemetria, args=(broker, id_ws), daemon=True)
    hilo.start()

    # Hilo principal para recepción de comandos (bloqueante)
    escuchar_comandos(broker, id_ws)