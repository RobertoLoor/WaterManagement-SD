# WM_Central/WM_Central.py
"""
Servidor central del sistema: coordina estaciones de riego, operarios y persistencia.
Recibe registros de Monitores vía sockets TCP, procesa peticiones y telemetría
mediante Kafka, emite el broadcast de red y arranca el panel web de control.
"""
import os
import sys
import socket
import threading
import time
import uuid

# Busca dinámicamente la carpeta raíz hacia arriba en el sistema de archivos para poder importar 'utils'.
_inicio = os.path.dirname(os.path.abspath(__file__))
dir_cursor = _inicio
while True:
    if os.path.isdir(os.path.join(dir_cursor, "utils")):
        if dir_cursor not in sys.path:
            # sys.path.insert(0, ...): añade la ruta para que Python busque ahí los paquetes a importar.
            sys.path.insert(0, dir_cursor)
        break
    parent = os.path.dirname(dir_cursor)
    if parent == dir_cursor:
        raise RuntimeError(f"No se encontro utils/. Inicio={_inicio}")
    dir_cursor = parent

from utils.protocol import (
    empaquetar_trama, desempaquetar_trama, recv_exact_frame,
    ENQ, ACK, NACK, EOT,
)
from utils.kafka_utils import (
    crear_productor, crear_consumidor,
    TOPIC_TELEMETRIA, TOPIC_COMANDOS, TOPIC_ESTADO_WS,
    TOPIC_PETICIONES, TOPIC_RESPUESTAS, TOPIC_BROADCAST,
)
from utils.db import Database

ESTADOS_QUE_PERMITEN_RIEGO = {"DISPONIBLE"}
riegos_activos = {}   # Diccionario en memoria: id_ws -> {request_id, id_operador, duracion_seg}
conexiones_ws = {}    # Guarda el socket TCP de cada Monitor conectado: id_ws -> socket
TIMEOUT_MONITOR = 15  # Segundos máximos sin recibir datos antes de dar al Monitor por caído

# Locks para evitar que hilos concurrentes modifiquen las variables compartidas al mismo tiempo.
lock_riegos = threading.Lock()
lock_conexiones = threading.Lock()

db: Database = None
productor = None


# Atiende la conexión TCP de un Monitor: procesa su registro y responde a los latidos de control.
def atender_monitor(conn, addr):
    print(f"[SOCKET] Nueva conexion de Monitor desde {addr}")
    id_registrado = None
    try:
        while True:
            # recv_exact_frame lee exactamente un byte de control o una trama completa STX...ETX+LRC.
            datos = recv_exact_frame(conn, timeout=TIMEOUT_MONITOR)
            if not datos:
                print(f"[SOCKET] Monitor {addr} desconectado.")
                break

            # Si el Monitor envía ENQ (latido), le respondemos ACK para confirmar que seguimos vivos.
            if datos == ENQ:
                conn.sendall(ACK)
                continue
            # EOT indica que el cliente quiere cerrar la transmisión ordenadamente.
            if datos == EOT:
                break

            es_valido, mensaje = desempaquetar_trama(datos)
            if not es_valido:
                print(f"[SOCKET] Trama corrupta de {addr}")
                conn.sendall(NACK)
                continue

            conn.sendall(ACK)
            print(f"[SOCKET] Mensaje de {addr}: {mensaje}")

            partes = mensaje.split('#')
            comando = partes[0]

            if comando == "REGISTRO":
                id_ws = partes[1] if len(partes) > 1 else "WS-DESCONOCIDA"
                ubicacion = partes[2] if len(partes) > 2 else "DESCONOCIDA"
                db.upsert_estacion_registro(id_ws, ubicacion)
                db.log_evento(f"{id_ws} registrada en '{ubicacion}' (a la espera de su Engine).")
                id_registrado = id_ws
                with lock_conexiones:
                    conexiones_ws[id_ws] = conn
                # Enviamos confirmación REGISTRO_OK empaquetada con checksum al Monitor.
                respuesta = empaquetar_trama(f"REGISTRO_OK#{id_ws}")
                conn.sendall(respuesta)

    except (ConnectionResetError, socket.timeout) as e:
        print(f"[SOCKET] Conexion {addr} interrumpida: {e}")
    except Exception as e:
        print(f"[SOCKET][ERROR] {addr}: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        # Si este socket era el que estaba activo para la estación, la damos de baja.
        es_vigente = False
        with lock_conexiones:
            if id_registrado and conexiones_ws.get(id_registrado) is conn:
                conexiones_ws.pop(id_registrado, None)
                es_vigente = True
        if es_vigente:
            try:
                tratar_desconexion(id_registrado, "El Monitor ha perdido la conexion con CENTRAL.")
            except Exception as e_desc:
                print(f"[SOCKET][ERROR] Error al tratar desconexion de {id_registrado}: {e_desc}")


# Arranca el servidor TCP para escuchar y aceptar conexiones de los Monitores de estación.
def servidor_sockets(puerto: int):
    # AF_INET = direcciones IPv4; SOCK_STREAM = protocolo TCP (conexión fiable y ordenada).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR permite volver a levantar el servidor en el mismo puerto sin esperar al SO.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # '0.0.0.0' indica que escuchamos en cualquier dirección de red de este equipo.
    s.bind(('0.0.0.0', puerto))
    s.listen()
    print(f"[WM_CENTRAL] Servidor de sockets escuchando en el puerto {puerto}...")
    while True:
        try:
            # accept() bloquea hasta que un cliente nuevo se conecta y devuelve el socket conn.
            conn, addr = s.accept()
            # Lanzamos cada cliente en su propio hilo daemon para que atiendan en paralelo.
            threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True).start()
        except Exception as e:
            print(f"[WM_CENTRAL][ERROR] Error en servidor_sockets: {e}")
            time.sleep(1)


# Publica un comando en el topic de Kafka dirigido a una estación de riego.
def enviar_comando(id_ws: str, comando: str, duracion_seg: int = None, operario: str = None):
    try:
        # key=id_ws garantiza que los mensajes hacia la misma estación viajen por la misma partición de Kafka.
        productor.send(TOPIC_COMANDOS, key=id_ws, value={
            "id_ws": id_ws, "comando": comando,
            "duracion_seg": duracion_seg, "operario": operario,
            "timestamp": time.time(),
        })
        # flush() vacía el buffer y fuerza el envío inmediato a la red.
        productor.flush()
    except Exception as e:
        print(f"[CENTRAL][ERROR] Error enviando comando a {id_ws}: {e}")


# Envía una respuesta o actualización a un operario específico a través de Kafka.
def responder_operario(id_operador: str, payload: dict):
    try:
        payload["id_operador"] = id_operador
        payload["timestamp"] = time.time()
        # Publicamos con clave id_operador para que el operario reciba sus respuestas en orden.
        productor.send(TOPIC_RESPUESTAS, key=id_operador, value=payload)
        productor.flush()
    except Exception as e:
        print(f"[CENTRAL][ERROR] Error respondiendo a operario {id_operador}: {e}")


# Hilo consumidor de Kafka que procesa solicitudes de riego enviadas por los operarios.
def hilo_peticiones(broker: str):
    consumidor = crear_consumidor(TOPIC_PETICIONES, broker, group_id="central-peticiones")
    print(f"[KAFKA] Escuchando peticiones en '{TOPIC_PETICIONES}'...")
    while True:
        try:
            # Itera sobre los mensajes que van llegando al topic desde Kafka.
            for msg in consumidor:
                try:
                    data = msg.value
                    if data.get("tipo") != "SOLICITUD_RIEGO":
                        continue
                    id_ws = data.get("id_ws")
                    id_operador = data.get("id_operador")
                    duracion = data.get("duracion_seg", 60)

                    if not id_ws or not id_operador:
                        print(f"[KAFKA][WARN] Mensaje SOLICITUD_RIEGO incompleto: {data}")
                        continue

                    try:
                        duracion = int(duracion)
                        if duracion <= 0:
                            raise ValueError
                    except (TypeError, ValueError):
                        duracion = 60

                    # UUID: genera un identificador aleatorio único si el mensaje no traía uno.
                    request_id = data.get("request_id", str(uuid.uuid4()))
                    db.upsert_operario(id_operador)

                    estacion = db.get_estacion(id_ws)
                    with lock_riegos:
                        ya_regando = id_ws in riegos_activos

                    # Caso 1: La estación no está registrada en la base de datos.
                    if not estacion:
                        responder_operario(id_operador, {
                            "request_id": request_id, "id_ws": id_ws, "evento": "DENEGADO",
                            "motivo": "La estacion no existe o nunca se ha conectado a CENTRAL.",
                        })
                        continue

                    # Caso 2: La estación no está en estado DISPONIBLE (ej. fuga o ya regando).
                    if ya_regando or estacion["estado"] not in ESTADOS_QUE_PERMITEN_RIEGO:
                        motivos = {
                            "REGANDO": "La estacion ya esta regando en este momento.",
                            "FUGA": "Fuga detectada: el riego esta bloqueado hasta resolver la averia.",
                            "FUERA_DE_SERVICIO": "La estacion esta fuera de servicio por orden de CENTRAL.",
                            "DESCONECTADA": "La estacion no esta conectada a CENTRAL.",
                        }
                        motivo = motivos.get(estacion["estado"], "La estacion no esta disponible.")
                        if ya_regando:
                            motivo = motivos["REGANDO"]
                        responder_operario(id_operador, {
                            "request_id": request_id, "id_ws": id_ws, "evento": "DENEGADO", "motivo": motivo,
                        })
                        db.log_evento(f"Riego DENEGADO en {id_ws} solicitado por {id_operador}: {motivo}")
                        continue

                    # Caso 3: Petición autorizada; registramos y enviamos la orden al Engine.
                    with lock_riegos:
                        riegos_activos[id_ws] = {
                            "request_id": request_id, "id_operador": id_operador, "duracion_seg": duracion,
                        }
                    db.set_estado(id_ws, "REGANDO", operario=id_operador)
                    enviar_comando(id_ws, "INICIAR_RIEGO", duracion_seg=duracion, operario=id_operador)
                    responder_operario(id_operador, {
                        "request_id": request_id, "id_ws": id_ws, "evento": "AUTORIZADO",
                        "motivo": "Riego autorizado e iniciado.", "duracion_seg": duracion,
                    })
                    db.log_evento(f"Riego AUTORIZADO en {id_ws} por {id_operador} durante {duracion}s.")

                except Exception as e:
                    print(f"[KAFKA][ERROR] hilo_peticiones procesando mensaje: {e}")
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_peticiones loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


# Hilo consumidor de Kafka que recibe lecturas de caudal y avisos de fin de riego de los Engines.
def hilo_telemetria(broker: str):
    consumidor = crear_consumidor(TOPIC_TELEMETRIA, broker, group_id="central-telemetria")
    print(f"[KAFKA] Escuchando telemetria en '{TOPIC_TELEMETRIA}'...")
    while True:
        try:
            for msg in consumidor:
                try:
                    data = msg.value
                    id_ws = data.get("id_ws")
                    if not id_ws:
                        continue
                    tipo = data.get("tipo", "TELEMETRIA")

                    # Lectura periódica durante el riego con caudal actual y volumen acumulado.
                    if tipo == "TELEMETRIA":
                        db.set_telemetria(id_ws, data.get("caudal_lmin", 0), data.get("volumen_total_l", 0))

                    # El Engine cerró la válvula: finalizamos el riego y enviamos resumen final al operario.
                    elif tipo == "RIEGO_FINALIZADO":
                        db.finalizar_riego(id_ws)
                        with lock_riegos:
                            info = riegos_activos.pop(id_ws, None)
                        motivo = data.get("motivo", "MANUAL")
                        db.log_evento(f"Riego finalizado en {id_ws} ({motivo}). Volumen total: {data.get('volumen_total_l', 0)} L.")
                        if info:
                            responder_operario(info["id_operador"], {
                                "request_id": info["request_id"], "id_ws": id_ws, "evento": "RESUMEN",
                                "motivo": motivo, "volumen_total_l": data.get("volumen_total_l", 0),
                                "duracion_seg": data.get("duracion_seg", 0),
                            })
                except Exception as e:
                    print(f"[KAFKA][ERROR] hilo_telemetria procesando mensaje: {e}")
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_telemetria loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


# Gestiona la caída de una estación: la marca desconectada y avisa al operario si estaba regando.
def tratar_desconexion(id_ws: str, motivo: str):
    with lock_riegos:
        info_riego = riegos_activos.pop(id_ws, None)
    db.marcar_desconectada(id_ws)
    db.log_evento(f"{id_ws} se ha desconectado de CENTRAL. {motivo}")
    # Si la estación se cayó en mitad de un riego, notificamos al operario para que no quede esperando.
    if info_riego:
        responder_operario(info_riego["id_operador"], {
            "request_id": info_riego["request_id"], "id_ws": id_ws,
            "evento": "INTERRUMPIDO", "motivo": "La estacion se ha desconectado.",
        })


# Hilo consumidor de Kafka que recibe cambios de estado (fugas, reconexiones) de los Monitores.
def hilo_estado_ws(broker: str):
    consumidor = crear_consumidor(TOPIC_ESTADO_WS, broker, group_id="central-estado-ws")
    print(f"[KAFKA] Escuchando estado de WS en '{TOPIC_ESTADO_WS}'...")
    while True:
        try:
            for msg in consumidor:
                try:
                    data = msg.value
                    id_ws = data.get("id_ws")
                    evento = data.get("evento")

                    if not id_ws or not evento:
                        continue

                    with lock_riegos:
                        info_riego = riegos_activos.get(id_ws)

                    # Si hay fuga, se bloquea la estación y se cancela cualquier riego en marcha.
                    if evento == "FUGA":
                        db.set_estado(id_ws, "FUGA")
                        db.log_evento(f"FUGA detectada en {id_ws}. Riego bloqueado.")
                        if info_riego:
                            enviar_comando(id_ws, "DETENER_RIEGO")
                            responder_operario(info_riego["id_operador"], {
                                "request_id": info_riego["request_id"], "id_ws": id_ws,
                                "evento": "INTERRUMPIDO", "motivo": "Fuga detectada durante el riego.",
                            })
                            with lock_riegos:
                                riegos_activos.pop(id_ws, None)

                    # Cuando la avería se repara, la estación vuelve a estar disponible para regar.
                    elif evento == "FUGA_RESUELTA":
                        if not info_riego:
                            db.set_estado(id_ws, "DISPONIBLE")
                        db.log_evento(f"Averia resuelta en {id_ws}. Estacion disponible de nuevo.")

                    elif evento == "DESCONECTADO":
                        tratar_desconexion(id_ws, "El Engine ha cortado la conexion.")

                    elif evento == "CONECTADO":
                        db.set_estado(id_ws, "DISPONIBLE")
                        db.log_evento(f"{id_ws} reconectada a CENTRAL.")

                except Exception as e:
                    print(f"[KAFKA][ERROR] hilo_estado_ws procesando mensaje: {e}")
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_estado_ws loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


# Difunde periódicamente el estado de todas las estaciones por Kafka para que lo lean los operarios.
def hilo_broadcast(intervalo: float = 2.0):
    while True:
        try:
            # Emite una instantánea completa de la red para que los FO no tengan que atacar la BD directamente.
            estaciones = db.get_todas_estaciones()
            productor.send(TOPIC_BROADCAST, value={"estaciones": estaciones, "timestamp": time.time()})
            productor.flush()
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_broadcast: {e}")
        time.sleep(intervalo)


# Inicia manualmente un riego en una estación desde el panel web de Central.
def accion_iniciar_riego(id_ws: str, duracion_seg: int = 60):
    estacion = db.get_estacion(id_ws)
    if not estacion or estacion["estado"] not in ESTADOS_QUE_PERMITEN_RIEGO:
        return False, "La estacion no esta disponible para iniciar riego."
    with lock_riegos:
        if id_ws in riegos_activos:
            return False, "La estacion ya esta regando en este momento."
        riegos_activos[id_ws] = {
            "request_id": str(uuid.uuid4()), "id_operador": "CENTRAL",
            "duracion_seg": duracion_seg
        }
    db.set_estado(id_ws, "REGANDO", operario="CENTRAL")
    enviar_comando(id_ws, "INICIAR_RIEGO", duracion_seg=duracion_seg, operario="CENTRAL")
    db.log_evento(f"Riego iniciado manualmente en {id_ws} desde CENTRAL.")
    return True, "Riego iniciado."


# Bloquea una estación poniéndola fuera de servicio e interrumpe cualquier riego activo.
def accion_bloquear(id_ws: str):
    estacion = db.get_estacion(id_ws)
    if not estacion:
        return False, "La estacion no existe."
    if estacion["estado"] == "DESCONECTADA":
        return False, "La estacion esta desconectada: no se puede bloquear."
    enviar_comando(id_ws, "BLOQUEAR")
    db.set_estado(id_ws, "FUERA_DE_SERVICIO")
    with lock_riegos:
        info_riego = riegos_activos.pop(id_ws, None)
    # Si había un riego activo, notificamos al operario para informarle de que su riego fue interrumpido.
    if info_riego:
        responder_operario(info_riego["id_operador"], {
            "request_id": info_riego["request_id"], "id_ws": id_ws,
            "evento": "INTERRUMPIDO", "motivo": "La estacion fue bloqueada por CENTRAL.",
        })
    db.log_evento(f"{id_ws} puesta FUERA DE SERVICIO por CENTRAL.")
    return True, "Estacion bloqueada."


# Desbloquea una estación que estaba fuera de servicio y la devuelve al estado DISPONIBLE.
def accion_activar(id_ws: str):
    estacion = db.get_estacion(id_ws)
    if not estacion:
        return False, "La estacion no existe."
    if estacion["estado"] == "DESCONECTADA":
        return False, "La estacion esta desconectada: no se puede activar."
    enviar_comando(id_ws, "ACTIVAR")
    db.set_estado(id_ws, "DISPONIBLE")
    db.log_evento(f"{id_ws} ACTIVADA de nuevo por CENTRAL.")
    return True, "Estacion activada."


# Función principal: lee la configuración, inicializa hilos de fondo y arranca el servidor web.
def main():
    global db, productor

    # Lee argumentos pasados por terminal (sys.argv) o variables de entorno (os.getenv) con valores por defecto.
    try:
        puerto_sockets = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("SOCKET_PORT", "5000"))
    except ValueError:
        puerto_sockets = 5000

    kafka_broker = sys.argv[2] if len(sys.argv) > 2 else os.getenv("KAFKA_BROKER", "localhost:9093")

    try:
        puerto_web = int(sys.argv[3]) if len(sys.argv) > 3 else int(os.getenv("PORT") or os.getenv("WEB_PORT") or "8080")
    except ValueError:
        puerto_web = 8080

    ruta_bd = os.getenv("DB_PATH", os.path.join(dir_cursor, "wm_central.db"))

    print("=" * 60)
    print(" WM_Central - WaterManagement")
    print(f" Puerto sockets : {puerto_sockets}")
    print(f" Kafka broker   : {kafka_broker}")
    print(f" Puerto web     : {puerto_web}")
    print(f" Base de datos  : {ruta_bd}")
    print("=" * 60)

    db = Database(ruta_bd)
    # Al arrancar, reseteamos a DESCONECTADA porque no sabemos qué estaciones siguen vivas.
    db.marcar_todas_desconectadas()

    productor = crear_productor(kafka_broker)

    # daemon=True hace que los hilos mueran solos cuando el programa principal termina.
    threading.Thread(target=servidor_sockets, args=(puerto_sockets,), daemon=True).start()
    threading.Thread(target=hilo_peticiones, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_telemetria, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_estado_ws, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_broadcast, daemon=True).start()

    from dashboard import crear_app
    app = crear_app(db, accion_iniciar_riego, accion_bloquear, accion_activar)
    # threaded=True permite a Flask manejar múltiples peticiones HTTP a la vez.
    app.run(host="0.0.0.0", port=puerto_web, threaded=True)


if __name__ == "__main__":
    main()
