from .base import baseProducer
import json
from typing import Iterable, Callable, Optional
from kafka import KafkaProducer as KafkaClient
from metadata import f1_topic
from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()

class KafkaProducer(baseProducer):
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.producer = KafkaClient(bootstrap_servers=bootstrap_servers,
                                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                                    batch_size=32768,
                                    linger_ms=10)

    def send(self, topic_name: f1_topic.F1Topic, message: dict, key: Optional[str] = None):
        self.producer.send(topic_name.value, key=key, value=message)

    def send_many(self, topic_name: f1_topic.F1Topic, messages: Iterable[dict] = None, key_builder: Optional[Callable[[dict], str]] = None):
        count = 0
        for message in messages or []:
            self.producer.send(topic_name.value, key=key_builder(message) if key_builder else None, value=message)
            count += 1
        logger.info(f"Sent {count} messages to topic '{topic_name.value}'.")
    
    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()
        
        