# utils/kafka_utils.py
import json
import time
from kafka import KafkaProducer, KafkaConsumer

# Manejo seguro para evitar el error: ImportError: cannot import name 'NoBrokersAvailable'
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    try:
        from kafka.errors import KafkaError as NoBrokersAvailable
    except ImportError:
        # Si por alguna razón extrema no existe, usamos Exception genérica como respaldo
        NoBrokersAvailable = Exception

# Topicos usados por todo el sistema (centralizados para evitar erratas)
TOPIC_TELEMETRIA = "telemetria-riego"       # Engine -> Central
TOPIC_COMANDOS = "comandos-estacion"        # Central -> Engine
TOPIC_ESTADO_WS = "estado-ws"               # Monitor -> Central
TOPIC_PETICIONES = "riego-peticiones"       # FO -> Central
TOPIC_RESPUESTAS = "riego-respuestas"       # Central -> FO
TOPIC_BROADCAST = "central-broadcast"       # Central -> FO / WS (estado global)


def crear_productor(broker: str, reintentos: int = 10, espera: int = 3) -> KafkaProducer:
    """Crea un productor de Kafka, reintentando si el broker aun no esta listo."""
    ultimo_error = None
    for intento in range(1, reintentos + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
            )
        except NoBrokersAvailable as e:
            ultimo_error = e
            print(f"[KAFKA] Broker no disponible aun ({intento}/{reintentos}). Reintentando en {espera}s...")
            time.sleep(espera)
    raise RuntimeError(f"No se pudo conectar al broker Kafka '{broker}': {ultimo_error}")


def crear_consumidor(topico, broker: str, group_id: str = None, desde_inicio: bool = False,
                      reintentos: int = 10, espera: int = 3) -> KafkaConsumer:
    """Crea un consumidor de Kafka. `topico` puede ser un str o una lista de topicos."""
    topicos = topico if isinstance(topico, (list, tuple)) else [topico]
    ultimo_error = None
    for intento in range(1, reintentos + 1):
        try:
            return KafkaConsumer(
                *topicos,
                bootstrap_servers=broker,
                group_id=group_id,
                auto_offset_reset='earliest' if desde_inicio else 'latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                consumer_timeout_ms=1000,
            )
        except NoBrokersAvailable as e:
            ultimo_error = e
            print(f"[KAFKA] Broker no disponible aun ({intento}/{reintentos}). Reintentando en {espera}s...")
            time.sleep(espera)
    raise RuntimeError(f"No se pudo conectar al broker Kafka '{broker}': {ultimo_error}")
