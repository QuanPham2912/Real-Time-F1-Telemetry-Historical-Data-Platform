from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from metadata.f1_topic import F1Topic
from metadata.logger import ETLLogger
from utils.config_reader import load_config
from .base import BaseTopicManager
from metadata.setting import get_settings

logger = ETLLogger.get_logger()
settings = get_settings()

CONFIG_PATH = "streaming.yaml"


class KafkaTopicManager(BaseTopicManager):
    def __init__(self, bootstrap_servers: str | None = None):
        if bootstrap_servers is None:
            bootstrap_servers = settings.kafka_bootstrap_servers

        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    def create_topic(
        self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1
    ):
        try:
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            self.admin_client.create_topics(new_topics=[topic], validate_only=False)
            logger.info(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            logger.warning(f"Topic '{topic_name}' already exists.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")

    def init_topics(self):
        topic_configs = load_config(CONFIG_PATH)["topics"]
        new_topics = []

        for topic_enum in F1Topic:
            topic_name = topic_enum.value
            config = topic_configs.get(
                topic_name, {"num_partitions": 1, "replication_factor": 1}
            )
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=config["num_partitions"],
                replication_factor=config["replication_factor"],
            )
            new_topics.append(new_topic)

        try:
            self.admin_client.create_topics(
                new_topics=new_topics, validate_only=False
            )
            logger.info("All topics initialized successfully.")
        except TopicAlreadyExistsError as e:
            logger.warning(f"Some topics already exist: {e}")
        except Exception as e:
            logger.error(f"Failed to initialize topics: {e}")

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

    def close(self):
        self.admin_client.close()
        logger.info("Kafka admin client closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
