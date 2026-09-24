# WM_Central/WM_Central.py
"""
WM_Central - Nucleo del sistema WaterManagement.
 
Responsabilidades:
 - Servidor de sockets (protocolo STX/ETX/LRC) para el registro/autenticacion
   de los Monitores de las estaciones de riego (WM_WS_M).
 - Productor/consumidores Kafka para:
     * recibir telemetria de riego de los Engines (WM_WS_E)
     * recibir incidencias (fugas/desconexiones) de los Monitores
     * recibir peticiones de activacion de los operarios (WM_FO)
     * enviar comandos a los Engines (iniciar/detener/bloquear/activar)
     * enviar respuestas a los operarios (autorizado/denegado/resumen)
     * emitir un "broadcast" periodico con el estado global de la red
 - Persistencia en SQLite (utils/db.py).
 - Panel de monitorizacion web (Flask) en tiempo real (polling 1s).
 
Uso:
    python WM_Central.py <puerto_sockets> <kafka_broker> [puerto_web]
 
Variables de entorno equivalentes (usadas por defecto si no hay argumentos,
utiles para Docker): SOCKET_PORT, KAFKA_BROKER, WEB_PORT, DB_PATH
"""
import os
import sys
import socket
import threading
import time
import uuid
 
# ---------------------------------------------------------------------------
# VISION GENERAL PARA ESTUDIAR
# ---------------------------------------------------------------------------
# WM_Central es el "cerebro". Trabaja con 5 hilos a la vez + el servidor web:
#
#   1. servidor_sockets  : acepta conexiones TCP de los Monitores (WM_WS_M)
#                          -> atender_monitor() por cada conexion (un hilo cada una)
#   2. hilo_peticiones   : lee topic 'riego-peticiones' (operarios piden regar)
#   3. hilo_telemetria   : lee topic 'telemetria-riego' (caudal/volumen del Engine)
#   4. hilo_estado_ws    : lee topic 'estado-ws' (fugas, desconexiones del Monitor)
#   5. hilo_broadcast    : cada 2 s publica el estado de todas las estaciones
#   +  Flask (dashboard) : el hilo principal sirve la web en el puerto 8080
#
# Flujo tipico de un riego:
#   WM_FO --(riego-peticiones)--> Central --valida--> --(comandos-estacion)--> Engine
#   Engine --(telemetria-riego)--> Central --> BD --> dashboard
#   Engine --(RIEGO_FINALIZADO)--> Central --(riego-respuestas RESUMEN)--> WM_FO
# ---------------------------------------------------------------------------
 
# --- localizar la raiz del proyecto (carpeta que contiene utils/) ---------
# Este script vive en WM_Central/, pero 'utils/' esta un nivel por encima. Subimos
# de carpeta en carpeta hasta encontrar 'utils' y anadimos esa carpeta al
# sys.path para que funcionen los "from utils.xxx import ...".
_inicio = os.path.dirname(os.path.abspath(__file__))
dir_cursor = _inicio
while True:
    if os.path.isdir(os.path.join(dir_cursor, "utils")):
        if dir_cursor not in sys.path:
            sys.path.insert(0, dir_cursor)
        break
    parent = os.path.dirname(dir_cursor)
    # Si al subir ya no cambia la carpeta, hemos llegado a la raiz del disco
    # sin encontrar 'utils': error, y el mensaje lista lo que hay para depurar.
    if parent == dir_cursor:
        raise RuntimeError(
            f"No se encontro utils/. Inicio={_inicio} | "
            f"contenido de {os.path.dirname(_inicio)}: "
            f"{sorted(os.listdir(os.path.dirname(_inicio)))}"
        )
    dir_cursor = parent
 
# Modulos propios (solo se pueden importar despues de arreglar el sys.path de arriba).
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
 
# ---------------------------------------------------------------- estado ---
# Solo se puede iniciar un riego si la estacion esta DISPONIBLE.
ESTADOS_QUE_PERMITEN_RIEGO = {"DISPONIBLE"}
# Riegos en curso en memoria: clave = id de la estacion. Guarda quien lo pidio
# y con que request_id, para poder responderle luego (resumen o interrupcion).
riegos_activos = {}   # id_ws -> {request_id, id_operador, duracion_seg}
# Lock para proteger 'riegos_activos', que tocan varios hilos a la vez.
lock_riegos = threading.Lock()
 
# Variables globales que se inicializan en main(): base de datos y productor Kafka.
db: Database = None
productor = None
 
 
# ============================================================ SOCKETS =====
def atender_monitor(conn, addr):
    """Atiende la conexion de un Monitor (WM_WS_M): ENQ -> ACK, registro -> alta en BD."""
    print(f"[SOCKET] Nueva conexion de Monitor desde {addr}")
    try:
        while True:
            # Lee una trama completa (o un caracter de control). Si el Monitor no
            # manda nada en 120 s salta socket.timeout (se captura mas abajo).
            datos = recv_exact_frame(conn, timeout=120)
            if not datos:
                # Cadena vacia = el Monitor cerro la conexion.
                print(f"[SOCKET] Monitor {addr} desconectado.")
                break
 
            # Dialogo del protocolo: ENQ ("¿estas ahi?") se responde con ACK.
            if datos == ENQ:
                conn.sendall(ACK)
                continue
            # EOT = el Monitor termina la comunicacion.
            if datos == EOT:
                break
 
            # Si es una trama de datos, comprobamos que el LRC es correcto.
            es_valido, mensaje = desempaquetar_trama(datos)
            if not es_valido:
                # Trama corrupta: pedimos que la reenvie con NACK.
                print(f"[SOCKET] Trama corrupta de {addr}")
                conn.sendall(NACK)
                continue
 
            # Trama valida: confirmamos con ACK y procesamos el contenido.
            conn.sendall(ACK)
            print(f"[SOCKET] Mensaje de {addr}: {mensaje}")
 
            # Los mensajes tienen la forma COMANDO#campo1#campo2...
            partes = mensaje.split('#')
            comando = partes[0]
 
            if comando == "REGISTRO":
                # REGISTRO#WS-01#River Park -> damos de alta (o reactivamos) la estacion.
                id_ws = partes[1] if len(partes) > 1 else "WS-DESCONOCIDA"
                ubicacion = partes[2] if len(partes) > 2 else "DESCONOCIDA"
                db.upsert_estacion_registro(id_ws, ubicacion)
                db.log_evento(f"{id_ws} registrada/conectada en '{ubicacion}'.")
                # Respondemos al Monitor con otra trama: REGISTRO_OK#WS-01
                respuesta = empaquetar_trama(f"REGISTRO_OK#{id_ws}")
                conn.sendall(respuesta)
    except (ConnectionResetError, socket.timeout) as e:
        # Errores "esperables": el cliente se cae o se queda mudo demasiado tiempo.
        print(f"[SOCKET] Conexion {addr} interrumpida: {e}")
    except Exception as e:
        # Cualquier otro error inesperado: se muestra sin tumbar el servidor.
        print(f"[SOCKET][ERROR] {addr}: {e}")
    finally:
        # Pase lo que pase, cerramos el socket de este cliente.
        conn.close()
 
 
def servidor_sockets(puerto: int):
    # Crea un socket TCP (AF_INET = IPv4, SOCK_STREAM = TCP).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR: permite reutilizar el puerto al reiniciar sin esperar.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # '0.0.0.0' = escuchar en todas las interfaces (necesario en Docker/Railway).
    s.bind(('0.0.0.0', puerto))
    s.listen()
    print(f"[WM_CENTRAL] Servidor de sockets escuchando en el puerto {puerto}...")
    while True:
        # accept() se bloquea hasta que llega una conexion nueva; a cada Monitor
        # se le asigna su propio hilo para poder atender varios a la vez.
        conn, addr = s.accept()
        threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True).start()
 
 
# ============================================================== KAFKA =====
def enviar_comando(id_ws: str, comando: str, duracion_seg: int = None, operario: str = None):
    # Publica una orden para el Engine de la estacion (INICIAR_RIEGO, DETENER_RIEGO,
    # BLOQUEAR, ACTIVAR). key=id_ws hace que todos los comandos de una estacion
    # vayan a la misma particion y se mantenga su orden.
    productor.send(TOPIC_COMANDOS, key=id_ws, value={
        "id_ws": id_ws, "comando": comando,
        "duracion_seg": duracion_seg, "operario": operario,
        "timestamp": time.time(),
    })
    # flush() fuerza el envio inmediato (send es asincrono y agrupa mensajes).
    productor.flush()
 
 
def responder_operario(id_operador: str, payload: dict):
    # Envia una respuesta (AUTORIZADO, DENEGADO, RESUMEN, INTERRUMPIDO) a un operario.
    # Se anade a quien va dirigida para que cada WM_FO filtre lo suyo.
    payload["id_operador"] = id_operador
    payload["timestamp"] = time.time()
    productor.send(TOPIC_RESPUESTAS, key=id_operador, value=payload)
    productor.flush()
 
 
def hilo_peticiones(broker: str):
    """Escucha las peticiones de riego que envian los operarios (WM_FO)."""
    # group_id fijo: Kafka recuerda hasta donde ha leido este grupo.
    consumidor = crear_consumidor(TOPIC_PETICIONES, broker, group_id="central-peticiones")
    print(f"[KAFKA] Escuchando peticiones en '{TOPIC_PETICIONES}'...")
    # while True + for: el consumidor tiene consumer_timeout_ms=1000, asi que el "for"
    # termina si pasa 1 s sin mensajes; el "while" lo vuelve a abrir (con un sleep corto).
    while True:
        for msg in consumidor:
            data = msg.value
            # Solo nos interesan las solicitudes de riego; el resto se ignora.
            if data.get("tipo") != "SOLICITUD_RIEGO":
                continue
            id_ws = data.get("id_ws")
            id_operador = data.get("id_operador")
            duracion = data.get("duracion_seg", 60)
            # Si el operario no manda request_id, se genera uno unico (UUID).
            request_id = data.get("request_id", str(uuid.uuid4()))
 
            # Consultamos el estado actual de la estacion y si ya hay un riego en curso.
            estacion = db.get_estacion(id_ws)
            with lock_riegos:
                ya_regando = id_ws in riegos_activos
 
            # CASO 1: la estacion no existe en la BD -> denegamos.
            if not estacion:
                responder_operario(id_operador, {
                    "request_id": request_id, "id_ws": id_ws, "evento": "DENEGADO",
                    "motivo": "La estacion no existe o nunca se ha conectado a CENTRAL.",
                })
                continue
 
            # CASO 2: existe pero no esta disponible (regando, fuga, bloqueada, desconectada).
            if ya_regando or estacion["estado"] not in ESTADOS_QUE_PERMITEN_RIEGO:
                # Mensaje de motivo segun el estado actual.
                motivos = {
                    "REGANDO": "La estacion ya esta regando en este momento.",
                    "FUGA": "Fuga detectada: el riego esta bloqueado hasta resolver la averia.",
                    "FUERA_DE_SERVICIO": "La estacion esta fuera de servicio por orden de CENTRAL.",
                    "DESCONECTADA": "La estacion no esta conectada a CENTRAL.",
                }
                motivo = motivos.get(estacion["estado"], "La estacion no esta disponible.")
                responder_operario(id_operador, {
                    "request_id": request_id, "id_ws": id_ws, "evento": "DENEGADO", "motivo": motivo,
                })
                db.log_evento(f"Riego DENEGADO en {id_ws} solicitado por {id_operador}: {motivo}")
                continue
 
            # CASO 3: todo correcto -> autorizar el riego.
            # 3a) Apuntamos el riego como activo en memoria.
            with lock_riegos:
                riegos_activos[id_ws] = {
                    "request_id": request_id, "id_operador": id_operador, "duracion_seg": duracion,
                }
            # 3b) Actualizamos la BD, mandamos la orden al Engine y avisamos al operario.
            db.set_estado(id_ws, "REGANDO", operario=id_operador)
            enviar_comando(id_ws, "INICIAR_RIEGO", duracion_seg=duracion, operario=id_operador)
            responder_operario(id_operador, {
                "request_id": request_id, "id_ws": id_ws, "evento": "AUTORIZADO",
                "motivo": "Riego autorizado e iniciado.", "duracion_seg": duracion,
            })
            db.log_evento(f"Riego AUTORIZADO en {id_ws} por {id_operador} durante {duracion}s.")
        time.sleep(0.2)
 
 
def hilo_telemetria(broker: str):
    """Escucha la telemetria y los resumenes finales que envian los Engines (WM_WS_E)."""
    consumidor = crear_consumidor(TOPIC_TELEMETRIA, broker, group_id="central-telemetria")
    print(f"[KAFKA] Escuchando telemetria en '{TOPIC_TELEMETRIA}'...")
    while True:
        for msg in consumidor:
            data = msg.value
            id_ws = data.get("id_ws")
            # Si el mensaje no indica tipo se asume que es telemetria normal.
            tipo = data.get("tipo", "TELEMETRIA")
 
            if tipo == "TELEMETRIA":
                # Lectura periodica durante el riego: guardamos caudal y volumen acumulado.
                db.set_telemetria(id_ws, data.get("caudal_lmin", 0), data.get("volumen_total_l", 0))
 
            elif tipo == "RIEGO_FINALIZADO":
                # El riego termino (por tiempo, manual, fuga...): estacion vuelve a DISPONIBLE.
                db.finalizar_riego(id_ws)
                # Sacamos el riego de la lista de activos (pop devuelve lo que habia, o None).
                with lock_riegos:
                    info = riegos_activos.pop(id_ws, None)
                motivo = data.get("motivo", "MANUAL")
                db.log_evento(f"Riego finalizado en {id_ws} ({motivo}). "
                               f"Volumen total: {data.get('volumen_total_l', 0)} L.")
                # Si sabemos quien lo pidio, le mandamos un RESUMEN con el volumen y duracion.
                if info:
                    responder_operario(info["id_operador"], {
                        "request_id": info["request_id"], "id_ws": id_ws, "evento": "RESUMEN",
                        "motivo": motivo, "volumen_total_l": data.get("volumen_total_l", 0),
                        "duracion_seg": data.get("duracion_seg", 0),
                    })
        time.sleep(0.2)
 
 
def hilo_estado_ws(broker: str):
    """Escucha eventos de salud (fuga/desconexion) enviados por los Monitores (WM_WS_M)."""
    consumidor = crear_consumidor(TOPIC_ESTADO_WS, broker, group_id="central-estado-ws")
    print(f"[KAFKA] Escuchando estado de WS en '{TOPIC_ESTADO_WS}'...")
    while True:
        for msg in consumidor:
            data = msg.value
            id_ws = data.get("id_ws")
            evento = data.get("evento")
 
            # Consultamos (sin modificar) si esta estacion tenia un riego en curso.
            with lock_riegos:
                info_riego = riegos_activos.get(id_ws)
 
            if evento == "FUGA":
                # Fuga: estado FUGA (bloquea nuevos riegos) y, si estaba regando, se detiene
                # y se avisa al operario de que su riego fue INTERRUMPIDO.
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
 
            elif evento == "FUGA_RESUELTA":
                # Averia arreglada: vuelve a DISPONIBLE (salvo que hubiera un riego activo).
                if not info_riego:
                    db.set_estado(id_ws, "DISPONIBLE")
                db.log_evento(f"Averia resuelta en {id_ws}. Estacion disponible de nuevo.")
 
            elif evento == "DESCONECTADO":
                # El Monitor se cayo: DESCONECTADA y, si habia riego, se avisa al operario.
                db.marcar_desconectada(id_ws)
                db.log_evento(f"{id_ws} se ha desconectado de CENTRAL.")
                if info_riego:
                    responder_operario(info_riego["id_operador"], {
                        "request_id": info_riego["request_id"], "id_ws": id_ws,
                        "evento": "INTERRUMPIDO", "motivo": "La estacion se ha desconectado.",
                    })
                    with lock_riegos:
                        riegos_activos.pop(id_ws, None)
 
            elif evento == "CONECTADO":
                # El Monitor volvio: la estacion pasa a DISPONIBLE.
                db.set_estado(id_ws, "DISPONIBLE")
                db.log_evento(f"{id_ws} reconectada a CENTRAL.")
        time.sleep(0.2)
 
 
def hilo_broadcast(intervalo: float = 2.0):
    """Publica periodicamente el estado global de la red (lo usan los WM_FO)."""
    while True:
        # Foto de todas las estaciones, publicada cada 'intervalo' segundos para que
        # los WM_FO puedan mostrar el estado de la red sin consultar la BD.
        estaciones = db.get_todas_estaciones()
        productor.send(TOPIC_BROADCAST, value={"estaciones": estaciones, "timestamp": time.time()})
        productor.flush()
        time.sleep(intervalo)
 
 
# ============================================================== ACCIONES ===
# API interna usada por el dashboard web (botones de CENTRAL, punto 11 del enunciado)
# Cada funcion devuelve (ok, mensaje). Se las pasamos a Flask en crear_app().
def accion_iniciar_riego(id_ws: str, duracion_seg: int = 60):
    # Boton "Regar": igual que aceptar una peticion, pero el operario es "CENTRAL".
    estacion = db.get_estacion(id_ws)
    if not estacion or estacion["estado"] not in ESTADOS_QUE_PERMITEN_RIEGO:
        return False, "La estacion no esta disponible para iniciar riego."
    with lock_riegos:
        riegos_activos[id_ws] = {"request_id": str(uuid.uuid4()), "id_operador": "CENTRAL",
                                  "duracion_seg": duracion_seg}
    db.set_estado(id_ws, "REGANDO", operario="CENTRAL")
    enviar_comando(id_ws, "INICIAR_RIEGO", duracion_seg=duracion_seg, operario="CENTRAL")
    db.log_evento(f"Riego iniciado manualmente en {id_ws} desde CENTRAL.")
    return True, "Riego iniciado."
 
 
def accion_bloquear(id_ws: str):
    # Boton "Bloquear": pone la estacion FUERA_DE_SERVICIO y cancela cualquier riego activo.
    enviar_comando(id_ws, "BLOQUEAR")
    db.set_estado(id_ws, "FUERA_DE_SERVICIO")
    with lock_riegos:
        riegos_activos.pop(id_ws, None)
    db.log_evento(f"{id_ws} puesta FUERA DE SERVICIO por CENTRAL.")
    return True, "Estacion bloqueada."
 
 
def accion_activar(id_ws: str):
    # Boton "Activar": deshace el bloqueo y deja la estacion DISPONIBLE.
    enviar_comando(id_ws, "ACTIVAR")
    db.set_estado(id_ws, "DISPONIBLE")
    db.log_evento(f"{id_ws} ACTIVADA de nuevo por CENTRAL.")
    return True, "Estacion activada."
 
 
# ================================================================= MAIN ====
def main():
    global db, productor
 
    # Cada parametro se toma, por orden de prioridad, de: argumento de linea de
    # comandos -> variable de entorno -> valor por defecto. En Railway se usan las
    # variables de entorno (SOCKET_PORT, KAFKA_BROKER, WEB_PORT, DB_PATH).
    puerto_sockets = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("SOCKET_PORT", 5000))
    kafka_broker = sys.argv[2] if len(sys.argv) > 2 else os.getenv("KAFKA_BROKER", "localhost:9093")
    puerto_web = int(sys.argv[3]) if len(sys.argv) > 3 else int(os.getenv("WEB_PORT", 8080))
    ruta_bd = os.getenv("DB_PATH", os.path.join(dir_cursor, "wm_central.db"))
 
    # Banner de arranque: es lo primero que ves en los logs de Railway.
    print("=" * 60)
    print(" WM_Central - WaterManagement")
    print(f" Puerto sockets : {puerto_sockets}")
    print(f" Kafka broker   : {kafka_broker}")
    print(f" Puerto web     : {puerto_web}")
    print(f" Base de datos  : {ruta_bd}")
    print("=" * 60)
 
    # 1) Base de datos y productor Kafka (si Kafka no responde, aqui salen los
    #    "Broker no disponible aun" y, agotados los reintentos, el programa termina).
    db = Database(ruta_bd)
    productor = crear_productor(kafka_broker)
 
    # 2) Arrancamos los hilos de fondo. daemon=True: se cierran solos al terminar el programa.
    threading.Thread(target=servidor_sockets, args=(puerto_sockets,), daemon=True).start()
    threading.Thread(target=hilo_peticiones, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_telemetria, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_estado_ws, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_broadcast, daemon=True).start()
 
    # 3) El dashboard web se arranca en el hilo principal y mantiene vivo el programa.
    # El dashboard web se importa aqui para evitar dependencias circulares
    from dashboard import crear_app
    app = crear_app(db, accion_iniciar_riego, accion_bloquear, accion_activar)
    app.run(host="0.0.0.0", port=puerto_web, threaded=True)
 
 
# Solo ejecuta main() si el fichero se lanza directamente (no si se importa).
if __name__ == "__main__":
    main()