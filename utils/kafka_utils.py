# utils/kafka_utils.py
"""
Centraliza la configuración y utilidades para comunicarse con Apache Kafka.
Define los nombres de topics compartidos y provee funciones para crear productores
y consumidores JSON tolerantes a fallos con reintentos automáticos.
"""
import json
import time
from kafka import KafkaProducer, KafkaConsumer

# Control de importación: según la versión de la librería kafka-python, la excepción de error cambia.
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    try:
        from kafka.errors import KafkaError as NoBrokersAvailable
    except ImportError:
        NoBrokersAvailable = Exception

# Topics del sistema (canales temáticos de Kafka donde se publican y leen mensajes)
TOPIC_TELEMETRIA = "telemetria-riego"       # Engine -> Central
TOPIC_COMANDOS   = "comandos-estacion"      # Central -> Engine
TOPIC_ESTADO_WS  = "estado-ws"              # Monitor -> Central
TOPIC_PETICIONES = "riego-peticiones"       # FO -> Central
TOPIC_RESPUESTAS = "riego-respuestas"       # Central -> FO
TOPIC_BROADCAST  = "central-broadcast"      # Central -> FO / WS


# Convierte los bytes recibidos de Kafka en un diccionario Python de forma segura.
def _deserializar_json(m):
    if not m:
        return {}
    try:
        # Decodifica los bytes a texto UTF-8 reemplazando caracteres ilegibles y luego parsea el JSON.
        return json.loads(m.decode('utf-8', errors='replace'))
    except Exception:
        return {}


# Crea y conecta un productor de Kafka configurado para enviar datos en formato JSON.
def crear_productor(broker: str, reintentos: int = 10, espera: int = 3) -> KafkaProducer:
    ultimo_error = None
    # Bucle con reintentos para dar tiempo a que el servidor de Kafka arranque si está iniciando.
    for intento in range(1, reintentos + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=broker,
                # value_serializer: convierte automáticamente cualquier diccionario Python a texto JSON binario.
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # key_serializer: Kafka usa la clave (ej. id_ws) para mandar mensajes de la misma estación a la misma partición.
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
        except (NoBrokersAvailable, Exception) as e:
            ultimo_error = e
            print(f"[KAFKA] Broker no disponible aun ({intento}/{reintentos}): {e}. Reintentando en {espera}s...")
            time.sleep(espera)
    raise RuntimeError(f"No se pudo conectar al broker Kafka '{broker}': {ultimo_error}")


# Crea un consumidor de Kafka para leer mensajes JSON de uno o varios topics.
def crear_consumidor(topico, broker: str, group_id: str = None, desde_inicio: bool = False,
                      reintentos: int = 10, espera: int = 3) -> KafkaConsumer:
    topicos = topico if isinstance(topico, (list, tuple)) else [topico]
    ultimo_error = None
    for intento in range(1, reintentos + 1):
        try:
            return KafkaConsumer(
                *topicos,
                bootstrap_servers=broker,
                # group_id: identificador de grupo. Consumidores con el mismo id se reparten la carga de trabajo.
                group_id=group_id,
                # 'earliest' lee mensajes antiguos acumulados si es un grupo nuevo; 'latest' solo escucha mensajes futuros.
                auto_offset_reset='earliest' if desde_inicio else 'latest',
                value_deserializer=_deserializar_json,
                key_deserializer=lambda k: k.decode('utf-8', errors='replace') if k else None,
                # Timeout de 1000ms: si no hay mensajes en 1s, el bucle cede el control para no bloquear el hilo para siempre.
                consumer_timeout_ms=1000,
            )
        except (NoBrokersAvailable, Exception) as e:
            ultimo_error = e
            print(f"[KAFKA] Broker no disponible aun ({intento}/{reintentos}): {e}. Reintentando en {espera}s...")
            time.sleep(espera)
    raise RuntimeError(f"No se pudo conectar al broker Kafka '{broker}': {ultimo_error}")
