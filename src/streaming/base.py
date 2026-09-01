from abc import ABC, abstractmethod

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