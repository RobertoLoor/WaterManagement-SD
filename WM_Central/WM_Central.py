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

Hilos y responsabilidades:
  1. servidor_sockets  : hilo daemon que acepta conexiones TCP de los Monitores
                         (WM_WS_M). Por cada conexion lanza un hilo atender_monitor().
  2. hilo_peticiones   : consumidor Kafka del topic 'riego-peticiones'.
                         Valida y autoriza (o deniega) las solicitudes de los operarios.
  3. hilo_telemetria   : consumidor Kafka del topic 'telemetria-riego'.
                         Actualiza caudal/volumen en BD y gestiona los avisos de fin de riego.
  4. hilo_estado_ws    : consumidor Kafka del topic 'estado-ws'.
                         Procesa eventos de salud enviados por los Monitores
                         (FUGA, FUGA_RESUELTA, CONECTADO, DESCONECTADO).
  5. hilo_broadcast    : cada 2 s publica el estado de todas las estaciones en
                         'central-broadcast' para que los WM_FO puedan mostrarlo.
  +  Flask (dashboard) : el hilo principal de Python sirve la web en el puerto web.

Flujo tipico de un riego:
  WM_FO --(riego-peticiones)--> Central --valida--> --(comandos-estacion)--> Engine
  Engine --(telemetria-riego)--> Central --> BD --> dashboard
  Engine --(RIEGO_FINALIZADO)--> Central --(riego-respuestas RESUMEN)--> WM_FO

Uso:
    python WM_Central.py <puerto_sockets> <kafka_broker> [puerto_web]

Variables de entorno equivalentes (usadas por defecto si no hay argumentos,
utiles para Docker): SOCKET_PORT, KAFKA_BROKER, WEB_PORT, DB_PATH

El puerto del dashboard web se toma de: argumento -> PORT (la variable que
asigna Railway) -> WEB_PORT -> 8080.
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
# Set (conjunto) de estados que permiten iniciar un riego.
# ¿Por qué un set y no una lista?
# La operación crítica es "estado in ESTADOS_QUE_PERMITEN_RIEGO", que en un set
# es O(1) (tiempo constante) frente a O(n) en una lista. Con los estados actuales
# la diferencia es insignificante, pero el set también expresa semánticamente
# que no importa el orden y que no hay duplicados: es la estructura correcta.
ESTADOS_QUE_PERMITEN_RIEGO = {"DISPONIBLE"}

# Diccionario de riegos en curso: clave = id_ws, valor = dict con:
#   {
#     "request_id"  : str  → UUID de la solicitud original de WM_FO (correlaciona
#                            petición con respuesta AUTORIZADO/RESUMEN/INTERRUMPIDO).
#     "id_operador" : str  → quién pidió el riego (para enviarle el RESUMEN al acabar).
#     "duracion_seg": int  → duración solicitada (informativa, el Engine la controla).
#   }
# Protegido por lock_riegos porque lo leen/escriben varios hilos a la vez.
riegos_activos = {}   # id_ws -> {request_id, id_operador, duracion_seg}

# Diccionario de sockets abiertos de cada Monitor: id_ws -> socket TCP.
# Mientras la conexión esté abierta y el Monitor mande latidos (ENQ cada 5 s),
# se sabe que la estación sigue viva. Si el socket se cierra o el Monitor enmudece
# más de TIMEOUT_MONITOR segundos, atender_monitor() llama a tratar_desconexion().
# Guardar el socket (en vez de solo un booleano) permite además detectar si una
# reconexión tardía sobreescribe la entrada del diccionario: si el socket actual
# no es el mismo objeto que el que teníamos al registrar, es una reconexión nueva.
conexiones_ws = {}

# Segundos sin recibir nada de un Monitor tras los cuales se le da por desconectado
# (el Monitor manda un latido ENQ cada 5 s).
TIMEOUT_MONITOR = 15

# Lock para proteger 'riegos_activos', que tocan varios hilos a la vez.
lock_riegos = threading.Lock()

# Variables globales que se inicializan en main(): base de datos y productor Kafka.
db: Database = None
productor = None


# ============================================================ SOCKETS =====
def atender_monitor(conn, addr):
    """
    Atiende la conexion persistente de un Monitor (WM_WS_M).

    Protocolo de registro (flujo ENQ->ACK->REGISTRO->REGISTRO_OK):
      1. Monitor envía ENQ ("¿estás ahí?") → CENTRAL responde ACK.
      2. Monitor envía trama REGISTRO#id_ws#ubicacion → CENTRAL responde ACK (LRC ok).
      3. CENTRAL envía trama REGISTRO_OK#id_ws (confirmación con el id asignado).
      4. La conexion queda ABIERTA: el Monitor manda ENQ periódicos (latidos) y
         CENTRAL responde ACK. Si el Monitor deja de mandar latidos más de
         TIMEOUT_MONITOR segundos, se da por caído.

    Se ejecuta en su propio hilo daemon (uno por Monitor conectado).
    """
    print(f"[SOCKET] Nueva conexion de Monitor desde {addr}")
    id_registrado = None   # id de la estacion que se registro por ESTA conexion
    try:
        while True:
            # Lee una trama completa (o un caracter de control). Si el Monitor no
            # manda nada en TIMEOUT_MONITOR s salta socket.timeout (se captura mas abajo).
            datos = recv_exact_frame(conn, timeout=TIMEOUT_MONITOR)
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
                db.log_evento(f"{id_ws} registrada en '{ubicacion}' (a la espera de su Engine).")
                # La conexion queda abierta: es la que indica que la estacion esta viva.
                id_registrado = id_ws
                conexiones_ws[id_ws] = conn
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
        # Si era la conexion vigente de una estacion, esa estacion queda DESCONECTADA.
        # (Si el Monitor ya se reconecto con otra conexion, no se toca.)
        if id_registrado and conexiones_ws.get(id_registrado) is conn:
            conexiones_ws.pop(id_registrado, None)
            tratar_desconexion(id_registrado, "El Monitor ha perdido la conexion con CENTRAL.")


def servidor_sockets(puerto: int):
    # Crea un socket TCP (AF_INET = IPv4, SOCK_STREAM = TCP).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR: permite reutilizar el puerto inmediatamente al reiniciar el proceso.
    # Sin esta opción, el SO puede mantener el puerto en estado TIME_WAIT hasta 4
    # minutos tras cerrar el proceso anterior, impidiendo arrancar de nuevo.
    # Indispensable en Railway, donde los reinicios son frecuentes.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # '0.0.0.0' = escuchar en TODAS las interfaces de red disponibles en el contenedor.
    # Necesario en Docker/Railway: si se pusiera '127.0.0.1' (loopback), los Monitores
    # externos no podrían conectarse porque solo alcanzarían la IP pública del contenedor.
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
    # ¿Por qué key=id_ws?
    # Kafka garantiza el orden de mensajes DENTRO de una misma partición.
    # Al usar el id de la estación como key, todos los comandos para esa estación
    # van a la misma partición y el Engine los recibe en el orden en que CENTRAL los envió.
    # Esto evita, por ejemplo, que un DETENER_RIEGO llegue antes que el INICIAR_RIEGO.
    productor.send(TOPIC_COMANDOS, key=id_ws, value={
        "id_ws": id_ws, "comando": comando,
        "duracion_seg": duracion_seg, "operario": operario,
        "timestamp": time.time(),
    })
    # flush() fuerza el envio inmediato (send es asincrono y agrupa mensajes).
    productor.flush()


def responder_operario(id_operador: str, payload: dict):
    """
    Envía una respuesta a un operario de campo (WM_FO) a través de Kafka.

    Se añade id_operador y timestamp al payload para que cada WM_FO pueda
    filtrar los mensajes que le corresponden (el topic es compartido por todos
    los operarios) y mostrar la hora correcta.

    Tipos de evento en el payload:
      AUTORIZADO   → el riego ha sido aprobado e iniciado.
      DENEGADO     → no se puede regar (estación ocupada, bloqueada, etc.).
      RESUMEN      → el riego terminó; incluye volumen total y duración real.
      INTERRUMPIDO → el riego fue cortado (fuga, desconexión, bloqueo manual).
    """
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
        try:
            for msg in consumidor:
                try:
                    data = msg.value
                    # Solo nos interesan las solicitudes de riego; el resto se ignora.
                    if data.get("tipo") != "SOLICITUD_RIEGO":
                        continue
                    id_ws = data.get("id_ws")
                    id_operador = data.get("id_operador")
                    duracion = data.get("duracion_seg", 60)
                    # Validacion de campos obligatorios.
                    if not id_ws or not id_operador:
                        print(f"[KAFKA][WARN] Mensaje SOLICITUD_RIEGO incompleto (falta id_ws o id_operador): {data}")
                        continue
                    # Duracion debe ser un entero positivo.
                    try:
                        duracion = int(duracion)
                        if duracion <= 0:
                            raise ValueError
                    except (TypeError, ValueError):
                        print(f"[KAFKA][WARN] duracion_seg invalida ({duracion!r}), usando 60s.")
                        duracion = 60
                    # Si el operario no manda request_id, se genera uno unico (UUID).
                    request_id = data.get("request_id", str(uuid.uuid4()))
                    # Alta del operario en la BD la primera vez que aparece (tabla 'operarios').
                    db.upsert_operario(id_operador)

                    # Consultamos el estado actual de la estacion y si ya hay un riego en curso.
                    estacion = db.get_estacion(id_ws)
                    with lock_riegos:
                        ya_regando = id_ws in riegos_activos

                    # CASO 1: la estacion no existe en la BD (nunca se ha registrado un Monitor
                    # con ese id_ws). No podemos enviar un comando a algo que no conocemos.
                    if not estacion:
                        responder_operario(id_operador, {
                            "request_id": request_id, "id_ws": id_ws, "evento": "DENEGADO",
                            "motivo": "La estacion no existe o nunca se ha conectado a CENTRAL.",
                        })
                        continue

                    # CASO 2: la estacion existe pero no está en un estado que permita regar.
                    # Puede estar: ya regando (riegos_activos), en fuga, bloqueada o desconectada.
                    # Se da un motivo específico según el estado para que el operario entienda por qué.
                    if ya_regando or estacion["estado"] not in ESTADOS_QUE_PERMITEN_RIEGO:
                        # Mensaje de motivo segun el estado actual.
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
                except Exception as e:
                    print(f"[KAFKA][ERROR] hilo_peticiones procesando mensaje: {e}")
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_peticiones loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


def hilo_telemetria(broker: str):
    """Escucha la telemetria y los resumenes finales que envian los Engines (WM_WS_E)."""
    consumidor = crear_consumidor(TOPIC_TELEMETRIA, broker, group_id="central-telemetria")
    print(f"[KAFKA] Escuchando telemetria en '{TOPIC_TELEMETRIA}'...")
    while True:
        try:
            for msg in consumidor:
                try:
                    data = msg.value
                    id_ws = data.get("id_ws")
                    if not id_ws:
                        print(f"[KAFKA][WARN] Mensaje de telemetria sin id_ws: {data}")
                        continue
                    # Si el mensaje no indica tipo se asume que es telemetria normal.
                    tipo = data.get("tipo", "TELEMETRIA")

                    if tipo == "TELEMETRIA":
                        # Lectura periódica durante el riego (cada ~1 segundo):
                        # guardamos el caudal instantáneo y el volumen acumulado en la BD.
                        # El dashboard de Flask lo lee en cada poll y actualiza la tabla.
                        db.set_telemetria(id_ws, data.get("caudal_lmin", 0), data.get("volumen_total_l", 0))

                    elif tipo == "RIEGO_FINALIZADO":
                        # El Engine ha terminado el riego (por tiempo, orden manual, fuga o bloqueo).
                        # A diferencia de TELEMETRIA, este tipo es un evento ÚNICO por riego:
                        # indica que la electroválvula se ha cerrado y el Engine ha publicado el resumen.
                        # Se finaliza el riego en BD (estado vuelve a DISPONIBLE si seguía REGANDO).
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
                except Exception as e:
                    print(f"[KAFKA][ERROR] hilo_telemetria procesando mensaje: {e}")
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_telemetria loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


def tratar_desconexion(id_ws: str, motivo: str):
    """
    Gestiona la desconexión de una estación de riego.

    Acciones que realiza:
      1. Elimina el riego activo de la memoria (si lo hubiera), para liberar
         la estación y no bloquear futuras peticiones.
      2. Marca la estación como DESCONECTADA en la BD.
      3. Registra el evento en el historial del dashboard.
      4. Si había un riego en curso, notifica al operario que su riego fue
         INTERRUMPIDO (con el motivo de la desconexión) para que sepa que
         la petición no llegó a completarse.

    Se llama desde dos lugares:
      - atender_monitor() → cuando la conexión TCP del Monitor se cierra o expira.
      - hilo_estado_ws()  → cuando el Monitor publica el evento DESCONECTADO por Kafka.
    """
    with lock_riegos:
        info_riego = riegos_activos.pop(id_ws, None)
    db.marcar_desconectada(id_ws)
    db.log_evento(f"{id_ws} se ha desconectado de CENTRAL. {motivo}")
    if info_riego:
        responder_operario(info_riego["id_operador"], {
            "request_id": info_riego["request_id"], "id_ws": id_ws,
            "evento": "INTERRUMPIDO", "motivo": "La estacion se ha desconectado.",
        })


def hilo_estado_ws(broker: str):
    """Escucha eventos de salud (fuga/desconexion) enviados por los Monitores (WM_WS_M)."""
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
                        print(f"[KAFKA][WARN] Mensaje estado-ws sin id_ws o evento: {data}")
                        continue

                    # Consultamos (sin modificar) si esta estacion tenia un riego en curso.
                    with lock_riegos:
                        info_riego = riegos_activos.get(id_ws)

                    if evento == "FUGA":
                        # El Monitor detectó que su Engine responde KO o no responde.
                        # → Estado FUGA en BD: bloquea nuevas peticiones de riego.
                        # → Si había riego activo: se ordena DETENER_RIEGO al Engine y se
                        #   notifica INTERRUMPIDO al operario. La fuga tiene prioridad
                        #   sobre cualquier riego en curso.
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
                        # La avería se ha reparado y el Engine vuelve a responder OK.
                        # → Solo se pone DISPONIBLE si NO hay riego activo (caso improbable
                        #   pero posible si un segundo hilo autorizó un riego justo en este
                        #   momento). Si hay riego, el estado lo gestiona hilo_telemetria.
                        if not info_riego:
                            db.set_estado(id_ws, "DISPONIBLE")
                        db.log_evento(f"Averia resuelta en {id_ws}. Estacion disponible de nuevo.")

                    elif evento == "DESCONECTADO":
                        # El Engine se desconectó del Monitor (el Monitor cerró la sesión
                        # con el Engine o el Engine cayó). La estación queda DESCONECTADA
                        # y, si había riego, el operario recibe INTERRUMPIDO.
                        tratar_desconexion(id_ws, "El Engine ha cortado la conexion.")

                    elif evento == "CONECTADO":
                        # El Monitor informa de que su Engine acaba de conectarse (o reconectarse).
                        # La estación pasa de DESCONECTADA a DISPONIBLE: ya puede recibir riegos.
                        db.set_estado(id_ws, "DISPONIBLE")
                        db.log_evento(f"{id_ws} reconectada a CENTRAL.")

                    else:
                        print(f"[KAFKA][WARN] Evento desconocido en estado-ws: {evento!r} para {id_ws}")

                except Exception as e:
                    print(f"[KAFKA][ERROR] hilo_estado_ws procesando mensaje: {e}")
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_estado_ws loop: {e}. Reintentando...")
            time.sleep(1)
        time.sleep(0.2)


def hilo_broadcast(intervalo: float = 2.0):
    """
    Publica periodicamente el estado global de la red en el topic 'central-broadcast'.

    ¿Para qué sirve el broadcast y quién lo consume?
    Los WM_FO no tienen acceso directo a la base de datos de CENTRAL (están en
    procesos distintos, potencialmente en máquinas distintas). Para que puedan
    mostrar el estado de todas las estaciones, CENTRAL publica periódicamente
    (cada 2 segundos) una "foto" completa con la lista de estaciones y sus
    estados actuales. Cada WM_FO la consume en su hilo_broadcast() y actualiza
    su diccionario local estaciones_conocidas.
    """
    while True:
        try:
            # Foto de todas las estaciones, publicada cada 'intervalo' segundos para que
            # los WM_FO puedan mostrar el estado de la red sin consultar la BD.
            estaciones = db.get_todas_estaciones()
            productor.send(TOPIC_BROADCAST, value={"estaciones": estaciones, "timestamp": time.time()})
            productor.flush()
        except Exception as e:
            print(f"[KAFKA][ERROR] hilo_broadcast: {e}")
        time.sleep(intervalo)


# ============================================================== ACCIONES ===
# API interna usada por el dashboard web (botones de CENTRAL, punto 11 del enunciado)
# Cada funcion devuelve (ok, mensaje). Se las pasamos a Flask en crear_app().
def accion_iniciar_riego(id_ws: str, duracion_seg: int = 60):
    """
    Inicia un riego manualmente desde el dashboard web de CENTRAL.

    Equivalente a recibir una petición de un operario, pero el "operario" es
    la propia CENTRAL. El riego se registra en riegos_activos con id_operador='CENTRAL',
    por lo que el resumen final también queda registrado (aunque no se envía a ningún
    WM_FO, dado que CENTRAL no es un operario de campo).

    :param id_ws:       Id de la estación a regar.
    :param duracion_seg: Duración del riego en segundos (defecto 60).
    :returns: (True, "Riego iniciado.") si tuvo éxito,
              (False, motivo) si la estación no está disponible.
    """
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
    """
    Bloquea una estación desde el dashboard (la pone FUERA_DE_SERVICIO).

    Si había un riego en curso, se cancela (el Engine recibe DETENER_RIEGO
    implícitamente al ponerse fuera de servicio). Se usa para mantenimiento
    o para prevenir el uso de una estación con problemas físicos.

    No se puede bloquear una estación DESCONECTADA: no sabemos si hay
    alguien escuchando en el otro extremo, y no tiene sentido cambiar su
    estado hasta que se reconecte.

    :param id_ws: Id de la estación a bloquear.
    :returns: (True, "Estacion bloqueada.") o (False, motivo).
    """
    # Boton "Bloquear": pone la estacion FUERA_DE_SERVICIO y cancela cualquier riego activo.
    estacion = db.get_estacion(id_ws)
    if not estacion:
        return False, "La estacion no existe."
    if estacion["estado"] == "DESCONECTADA":
        # No se puede dar una orden a una estacion que no esta conectada ni inventar su estado.
        return False, "La estacion esta desconectada: no se puede bloquear."
    enviar_comando(id_ws, "BLOQUEAR")
    db.set_estado(id_ws, "FUERA_DE_SERVICIO")
    with lock_riegos:
        riegos_activos.pop(id_ws, None)
    db.log_evento(f"{id_ws} puesta FUERA DE SERVICIO por CENTRAL.")
    return True, "Estacion bloqueada."


def accion_activar(id_ws: str):
    """
    Desbloquea una estación previamente puesta FUERA_DE_SERVICIO.

    La pone de nuevo en DISPONIBLE y envía el comando ACTIVAR al Engine
    para que levante el bloqueo en su lado también.

    No se puede activar una estación DESCONECTADA (mismo motivo que en bloquear).

    :param id_ws: Id de la estación a activar.
    :returns: (True, "Estacion activada.") o (False, motivo).
    """
    # Boton "Activar": deshace el bloqueo y deja la estacion DISPONIBLE.
    estacion = db.get_estacion(id_ws)
    if not estacion:
        return False, "La estacion no existe."
    if estacion["estado"] == "DESCONECTADA":
        return False, "La estacion esta desconectada: no se puede activar."
    enviar_comando(id_ws, "ACTIVAR")
    db.set_estado(id_ws, "DISPONIBLE")
    db.log_evento(f"{id_ws} ACTIVADA de nuevo por CENTRAL.")
    return True, "Estacion activada."


# ================================================================= MAIN ====
def main():
    global db, productor

    # Cada parametro se toma, por orden de prioridad, de: argumento de linea de
    # comandos -> variable de entorno -> valor por defecto. En Railway se usan las
    # variables de entorno (SOCKET_PORT, KAFKA_BROKER, DB_PATH y PORT para la web:
    # Railway asigna PORT y el dashboard debe escuchar en 0.0.0.0:$PORT).
    puerto_sockets = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("SOCKET_PORT", 5000))
    kafka_broker = sys.argv[2] if len(sys.argv) > 2 else os.getenv("KAFKA_BROKER", "localhost:9093")
    puerto_web = int(sys.argv[3]) if len(sys.argv) > 3 else int(os.getenv("PORT") or os.getenv("WEB_PORT") or 8080)
    ruta_bd = os.getenv("DB_PATH", os.path.join(dir_cursor, "wm_central.db"))

    # Banner de arranque: es lo primero que ves en los logs de Railway.
    print("=" * 60)
    print(" WM_Central - WaterManagement")
    print(f" Puerto sockets : {puerto_sockets}")
    print(f" Kafka broker   : {kafka_broker}")
    print(f" Puerto web     : {puerto_web}")
    print(f" Base de datos  : {ruta_bd}")
    print("=" * 60)

    # PASO 1: Inicializar la base de datos.
    # Si Kafka no responde al crear el productor, aquí salen los mensajes
    # "Broker no disponible aun" y, agotados los reintentos, el programa termina.
    db = Database(ruta_bd)
    # Al (re)iniciar CENTRAL no sabemos el estado real de las estaciones: las marcamos
    # todas DESCONECTADAS. Cada Monitor que se reconecte actualizará su estado.
    db.marcar_todas_desconectadas()

    # PASO 2: Crear el productor Kafka (puede tardar si el broker aún no está listo).
    productor = crear_productor(kafka_broker)

    # PASO 3: Arrancar los hilos de fondo en modo daemon.
    # daemon=True: al terminar el proceso principal (Flask) todos los hilos
    # se cierran automáticamente, sin necesidad de pararlos manualmente.
    threading.Thread(target=servidor_sockets, args=(puerto_sockets,), daemon=True).start()
    threading.Thread(target=hilo_peticiones, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_telemetria, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_estado_ws, args=(kafka_broker,), daemon=True).start()
    threading.Thread(target=hilo_broadcast, daemon=True).start()

    # PASO 4: Arrancar el dashboard web en el hilo principal.
    # Flask.run() es bloqueante: mantiene vivo el proceso mientras sirve la web.
    # threaded=True permite atender varias peticiones HTTP en paralelo (polling).
    # El import se hace aquí (y no al principio) para evitar importaciones
    # circulares: dashboard.py no importa WM_Central, pero si se pusiera el
    # import arriba del fichero podría causar problemas con el sys.path.
    from dashboard import crear_app
    app = crear_app(db, accion_iniciar_riego, accion_bloquear, accion_activar)
    app.run(host="0.0.0.0", port=puerto_web, threaded=True)


# Solo ejecuta main() si el fichero se lanza directamente (no si se importa).
if __name__ == "__main__":
    main()
