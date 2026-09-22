# utils/kafka_utils.py
import json
from kafka import KafkaProducer, KafkaConsumer

def crear_productor(broker: str) -> KafkaProducer:
    """Crea un productor de Kafka configurado para enviar objetos JSON."""
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def crear_consumidor(topico: str, broker: str, group_id: str = None) -> KafkaConsumer:
    """Crea un consumidor de Kafka configurado para recibir objetos JSON."""
    return KafkaConsumer(
        topico,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
