from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from metadata.f1_topic import F1Topic
from metadata.logger import ETLLogger
from .base import BaseTopicManager

logger = ETLLogger.get_logger()

class KafkaTopicManager(BaseTopicManager):

    TOPIC_CONFIGS = {
        F1Topic.DIM_DRIVER: {"num_partitions": 1, "replication_factor": 1},
        F1Topic.DIM_CONSTRUCTOR: {"num_partitions": 1, "replication_factor": 1},
        F1Topic.FACT_RACE_RESULT: {"num_partitions": 3, "replication_factor": 1},
        F1Topic.FACT_LAP: {"num_partitions": 3, "replication_factor": 1},
        F1Topic.STREAM_TELEMETRY: {"num_partitions": 6, "replication_factor": 1},
        F1Topic.STREAM_WEATHER: {"num_partitions": 1, "replication_factor": 1}
    }

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        try:
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            self.admin_client.create_topics(new_topics=[topic], validate_only=False)
            logger.info(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            logger.warning(f"Topic '{topic_name}' already exists.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
        

    def delete_topic(self, topic_name: str):
        try:
            self.admin_client.delete_topics(topics=[topic_name])
            logger.info(f"Topic '{topic_name}' deleted successfully.")
        except Exception as e:
            logger.error(f"Failed to delete topic '{topic_name}': {e}")

    def list_topics(self) -> list:
        try:
            topics = self.admin_client.list_topics()
            return topics
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []

    def init_topics(self):
        new_topics = [
            NewTopic(name=topic.value,
                     num_partitions=config["num_partitions"],
                     replication_factor=config["replication_factor"])
            for topic, config in self.TOPIC_CONFIGS.items()
        ]

        try:
            self.admin_client.create_topics(new_topics=new_topics, validate_only=False)
            logger.info("All topics initialized successfully.")
        except TopicAlreadyExistsError as e:
            logger.warning(f"Some topics already exist: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize topics: {e}")

    def close(self):
        self.admin_client.close()
        logger.info("Kafka admin client closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
