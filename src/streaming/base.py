from abc import ABC, abstractmethod
from metadata import f1_topic
from typing import Iterable, Callable, Optional

class BaseTopicManager(ABC):
    @abstractmethod
    def create_topic(self, topic_name: str, num_partitions: int, replication_factor: int):
        pass

    @abstractmethod
    def delete_topic(self, topic_name: str):
        pass

    @abstractmethod
    def list_topics(self) -> list:
        pass

    @abstractmethod
    def init_topics(self):
        pass

    @abstractmethod
    def close(self):
        pass

class baseProducer(ABC):
    @abstractmethod
    def send(self, topic_name: f1_topic.F1Topic, message: dict, key: Optional[str] = None):
        pass

    @abstractmethod
    def send_many(self, topic_name: f1_topic.F1Topic, messages: Iterable[dict] = None, key_builder: Optional[Callable[[dict], str]] = None):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def flush(self):
        pass