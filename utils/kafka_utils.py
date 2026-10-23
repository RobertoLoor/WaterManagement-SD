# utils/kafka_utils.py
"""
Funciones y constantes de Kafka compartidas por todos los componentes
(WM_Central, WM_WS_M, WM_WS_E y WM_FO).

¿Qué es Kafka?
--------------
Apache Kafka es un sistema de mensajería distribuido basado en el concepto
de "log de eventos". Los mensajes se organizan en TOPICS (canales con nombre);
cualquier componente puede escribir (producir) o leer (consumir) de ellos.

Conceptos clave:
  - TOPIC      : canal con nombre. Los mensajes se acumulan en él de forma
                 persistente (por defecto 7 días).
  - PRODUCTOR  : componente que escribe mensajes en un topic.
  - CONSUMIDOR : componente que lee mensajes de un topic.
  - GROUP_ID   : identificador de grupo de consumo. Kafka recuerda hasta
                 qué mensaje (OFFSET) ha leído cada grupo; si el consumidor
                 se reinicia, retoma desde donde lo dejó.
  - OFFSET     : posición de un mensaje dentro del topic (número correlativo
                 desde 0). Kafka lo gestiona por group_id de forma automática.
  - PARTICIÓN  : subdivisión interna de un topic. Los mensajes con la misma
                 KEY siempre van a la misma partición, garantizando orden
                 relativo entre mensajes del mismo emisor.

Aquí se centralizan los nombres de topics y la creación de productores y
consumidores con reintentos (porque Kafka puede tardar en arrancar en
Railway/Docker antes de que los microservicios lo intenten usar).
"""
import json
import time
from kafka import KafkaProducer, KafkaConsumer

# ---------------------------------------------------------------------------
# RESUMEN PARA ESTUDIAR
# ---------------------------------------------------------------------------
# Kafka es un "buzon" de mensajes organizado en TOPICS (canales con nombre).
#  - Un PRODUCTOR escribe mensajes en un topic.
#  - Un CONSUMIDOR lee mensajes de un topic.
#  - Un consumidor pertenece a un GROUP_ID: Kafka apunta hasta que mensaje ha
#    leido cada grupo (offset), de modo que si se reinicia no repite mensajes.
# Aqui se centralizan los nombres de topics y la creacion de productores y
# consumidores con reintentos (porque Kafka puede tardar en arrancar).
# ---------------------------------------------------------------------------

# Manejo seguro para evitar el error: ImportError: cannot import name 'NoBrokersAvailable'
# Segun la version de la libreria, esta excepcion puede estar en sitios distintos
# o no existir. Probamos varias opciones, de la mas concreta a la mas generica.
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    try:
        from kafka.errors import KafkaError as NoBrokersAvailable
    except ImportError:
        # Si por alguna razón extrema no existe, usamos Exception genérica como respaldo
        NoBrokersAvailable = Exception

# ---------------------------------------------------------------------------
# TOPICS del sistema (centralizados para evitar erratas en los nombres)
# Cada topic tiene un único productor "oficial" y uno o más consumidores.
# Formato del comentario: "Quién escribe -> Quién lee"
# ---------------------------------------------------------------------------

# TOPIC_TELEMETRIA: el Engine (WM_WS_E) publica cada segundo el caudal y el
# volumen acumulado mientras riega; también publica el mensaje RIEGO_FINALIZADO
# al terminar. WM_Central lo lee en hilo_telemetria() para actualizar la BD.
TOPIC_TELEMETRIA = "telemetria-riego"       # Engine -> Central

# TOPIC_COMANDOS: WM_Central publica órdenes (INICIAR_RIEGO, DETENER_RIEGO,
# BLOQUEAR, ACTIVAR) dirigidas a un Engine concreto (campo id_ws). Todos los
# Engines leen este topic, pero solo atienden los mensajes con su propio id_ws.
TOPIC_COMANDOS = "comandos-estacion"        # Central -> Engine

# TOPIC_ESTADO_WS: el Monitor (WM_WS_M) publica eventos de salud del Engine
# (CONECTADO, DESCONECTADO, FUGA, FUGA_RESUELTA). WM_Central los lee en
# hilo_estado_ws() para actualizar el estado de la estación en la BD.
TOPIC_ESTADO_WS = "estado-ws"               # Monitor -> Central

# TOPIC_PETICIONES: WM_FO publica solicitudes de riego (SOLICITUD_RIEGO).
# WM_Central las lee en hilo_peticiones(), las valida y autoriza o deniega.
TOPIC_PETICIONES = "riego-peticiones"       # FO -> Central

# TOPIC_RESPUESTAS: WM_Central publica las respuestas a las solicitudes de los
# operarios (AUTORIZADO, DENEGADO, RESUMEN, INTERRUMPIDO). Cada WM_FO filtra
# los mensajes por su propio id_operador.
TOPIC_RESPUESTAS = "riego-respuestas"       # Central -> FO

# TOPIC_BROADCAST: WM_Central publica cada 2 segundos el estado completo de
# todas las estaciones. WM_FO lo consume para mostrar la tabla de estaciones
# sin tener acceso directo a la BD.
TOPIC_BROADCAST = "central-broadcast"       # Central -> FO / WS (estado global)


def crear_productor(broker: str, reintentos: int = 10, espera: int = 3) -> KafkaProducer:
    """
    Crea y devuelve un KafkaProducer listo para enviar mensajes.

    Reintenta la conexión hasta 'reintentos' veces con 'espera' segundos
    entre intentos, porque en Railway/Docker el broker Kafka puede tardar
    unos segundos en estar listo cuando el contenedor acaba de arrancar.

    Parámetros del KafkaProducer:
      - bootstrap_servers : dirección "host:puerto" del broker al que conectarse.
                            En Railway se configura con la variable KAFKA_BROKER.
      - value_serializer  : función que convierte el valor (un dict Python) a bytes
                            para enviarlo por la red. Aquí: dict -> JSON string -> UTF-8.
      - key_serializer    : función que convierte la key (str o None) a bytes.
                            La key es opcional pero, si se usa, Kafka envía todos
                            los mensajes con la misma key a la misma partición,
                            lo que garantiza orden relativo (importante para comandos
                            dirigidos a una misma estación).

    :param broker:     Dirección del broker, p. ej. "localhost:9093".
    :param reintentos: Número máximo de intentos de conexión (defecto 10).
    :param espera:     Segundos entre intentos (defecto 3 → hasta 30 s en total).
    :returns: KafkaProducer configurado y conectado.
    :raises RuntimeError: Si se agotan los reintentos sin éxito.
    """
    ultimo_error = None
    # Hasta 'reintentos' intentos, esperando 'espera' segundos entre ellos
    # (por defecto, 10 x 3 s = 30 s). Los "Broker no disponible aun (x/10)" de
    # tus logs salen de aqui.
    for intento in range(1, reintentos + 1):
        try:
            return KafkaProducer(
                # Direccion "host:puerto" del broker (en Railway: la interna o la del proxy).
                bootstrap_servers=broker,
                # Los mensajes son diccionarios Python: se convierten a JSON y luego a bytes.
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # La "key" es opcional; si se usa (p. ej. el id de la estacion) Kafka
                # manda los mensajes con la misma key a la misma particion (mantiene el orden).
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
        except NoBrokersAvailable as e:
            # No se pudo conectar: guardamos el error, avisamos y esperamos.
            ultimo_error = e
            print(f"[KAFKA] Broker no disponible aun ({intento}/{reintentos}). Reintentando en {espera}s...")
            time.sleep(espera)
    # Si se agotan los reintentos, se lanza un error y el programa termina
    # (Railway lo reiniciara segun la politica de reinicio).
    raise RuntimeError(f"No se pudo conectar al broker Kafka '{broker}': {ultimo_error}")


def crear_consumidor(topico, broker: str, group_id: str = None, desde_inicio: bool = False,
                      reintentos: int = 10, espera: int = 3) -> KafkaConsumer:
    """
    Crea y devuelve un KafkaConsumer suscrito al topic (o lista de topics) indicado.

    Parámetros clave del KafkaConsumer:
      - bootstrap_servers    : igual que en el productor.
      - group_id             : identifica al grupo de consumo.
                               · None     → sin grupo; Kafka no guarda progreso.
                                            Cada instancia recibe sus propios mensajes
                                            (útil en WM_FO/hilo_broadcast, donde no
                                            importa repetir mensajes tras reiniciar).
                               · str fijo → grupo persistente; Kafka recuerda el offset
                                            (p. ej. "central-peticiones" en WM_Central).
                               · str único (con UUID) → cada ejecución empieza desde
                                            el punto que indique auto_offset_reset,
                                            sin compartir offset con otras instancias
                                            (útil en WM_FO/hilo_respuestas).
      - auto_offset_reset    : qué hacer si el grupo no tiene offset guardado aún:
                               · 'earliest' → leer desde el mensaje más antiguo del topic
                                              (no se pierde ninguna respuesta aunque el
                                              consumidor arranque tarde).
                               · 'latest'   → leer solo los mensajes nuevos (defecto cuando
                                              desde_inicio=False). Adecuado cuando solo
                                              interesan los mensajes futuros.
      - consumer_timeout_ms  : tiempo máximo (ms) que el iterador "for msg in consumidor"
                               espera un mensaje antes de parar. Con 1000 ms el bucle
                               termina si pasa 1 s sin mensajes, lo que permite que el
                               "while True" externo compruebe el estado del hilo y evita
                               bloqueos indefinidos (el hilo sigue vivo).

    :param topico:       Nombre del topic (str) o lista de nombres ([str]).
    :param broker:       Dirección del broker.
    :param group_id:     Identificador del grupo de consumo (None = sin grupo).
    :param desde_inicio: True → auto_offset_reset='earliest'; False → 'latest'.
    :param reintentos:   Número máximo de intentos de conexión.
    :param espera:       Segundos entre intentos.
    :returns: KafkaConsumer configurado y conectado.
    :raises RuntimeError: Si se agotan los reintentos sin éxito.
    """
    # Aceptamos un topic suelto ("a") o una lista (["a","b"]); lo normalizamos a lista.
    topicos = topico if isinstance(topico, (list, tuple)) else [topico]
    ultimo_error = None
    for intento in range(1, reintentos + 1):
        try:
            return KafkaConsumer(
                *topicos,                       # topics a los que se suscribe
                bootstrap_servers=broker,       # direccion del broker
                # group_id: identifica al grupo de consumo. Con un grupo, Kafka recuerda
                # por donde ibas (offset). Con group_id=None no guarda progreso.
                group_id=group_id,
                # Que hacer si el grupo no tiene offset guardado (primera vez):
                #  'earliest' = leer desde el principio; 'latest' = solo mensajes nuevos.
                auto_offset_reset='earliest' if desde_inicio else 'latest',
                # Operacion inversa a la del productor: bytes -> JSON -> diccionario.
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                # IMPORTANTE: si pasan 1000 ms sin mensajes, el bucle "for msg in consumidor"
                # TERMINA (no se queda bloqueado para siempre). Por eso los hilos de
                # WM_Central envuelven el "for" en un "while True" con un pequeno sleep.
                consumer_timeout_ms=1000,
            )
        except NoBrokersAvailable as e:
            ultimo_error = e
            print(f"[KAFKA] Broker no disponible aun ({intento}/{reintentos}). Reintentando en {espera}s...")
            time.sleep(espera)
    raise RuntimeError(f"No se pudo conectar al broker Kafka '{broker}': {ultimo_error}")
