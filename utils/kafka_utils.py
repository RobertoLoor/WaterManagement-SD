# utils/kafka_utils.py
"""
Funciones y constantes de Kafka compartidas por todos los componentes
(WM_Central, WM_WS_M, WM_WS_E y WM_FO).
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
 
# Topicos usados por todo el sistema (centralizados para evitar erratas)
# Se definen una sola vez para que todos los componentes usen exactamente
# el mismo nombre. Comentario: "Origen -> Destino".
TOPIC_TELEMETRIA = "telemetria-riego"       # Engine -> Central
TOPIC_COMANDOS = "comandos-estacion"        # Central -> Engine
TOPIC_ESTADO_WS = "estado-ws"               # Monitor -> Central
TOPIC_PETICIONES = "riego-peticiones"       # FO -> Central
TOPIC_RESPUESTAS = "riego-respuestas"       # Central -> FO
TOPIC_BROADCAST = "central-broadcast"       # Central -> FO / WS (estado global)
 
 
def crear_productor(broker: str, reintentos: int = 10, espera: int = 3) -> KafkaProducer:
    """Crea un productor de Kafka, reintentando si el broker aun no esta listo."""
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
    """Crea un consumidor de Kafka. `topico` puede ser un str o una lista de topicos."""
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
 