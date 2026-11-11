from types import SimpleNamespace
import pytest
from unittest.mock import Mock, MagicMock, patch, call
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import NewTopic
import streaming.topic_manager as topic_manager_module
from streaming.topic_manager import KafkaTopicManager
from streaming.base import BaseTopicManager
from metadata.f1_topic import F1Topic


class TestKafkaTopicManager:
    """Test suite for KafkaTopicManager class."""

    @pytest.fixture
    def mock_admin_client(self):
        """Fixture to provide a mocked KafkaAdminClient."""
        with patch('streaming.topic_manager.KafkaAdminClient') as mock_client:
            yield mock_client

    @pytest.fixture
    def topic_configs(self, monkeypatch):
        configs = {
            topic.value: {
                "num_partitions": index + 1,
                "replication_factor": 2,
            }
            for index, topic in enumerate(F1Topic)
        }
        monkeypatch.setattr(
            topic_manager_module,
            "load_config",
            Mock(return_value={"topics": configs}),
        )
        return configs

    @pytest.fixture
    def kafka_manager(self, mock_admin_client, topic_configs):
        """Fixture to provide a KafkaTopicManager instance with mocked admin client."""
        mock_instance = MagicMock()
        mock_admin_client.return_value = mock_instance
        manager = KafkaTopicManager(bootstrap_servers="localhost:9092")
        manager.admin_client = mock_instance
        return manager

    def test_kafka_topic_manager_is_subclass_of_base(self):
        """Test that KafkaTopicManager is a subclass of BaseTopicManager."""
        assert issubclass(KafkaTopicManager, BaseTopicManager)

    def test_init_with_bootstrap_servers_from_environment(
        self, mock_admin_client, monkeypatch
    ):
        """Test KafkaTopicManager reads bootstrap servers from the environment."""
        monkeypatch.setattr(
            topic_manager_module,
            "settings",
            SimpleNamespace(kafka_bootstrap_servers="broker-from-settings:9092"),
        )
        KafkaTopicManager()

        mock_admin_client.assert_called_once_with(
            bootstrap_servers="broker-from-settings:9092"
        )

    def test_init_with_default_bootstrap_servers_when_environment_is_missing(
        self, mock_admin_client, monkeypatch
    ):
        """Test KafkaTopicManager uses the default when no bootstrap env is set."""
        monkeypatch.setattr(
            topic_manager_module,
            "settings",
            SimpleNamespace(kafka_bootstrap_servers="kafka1:19092"),
        )
        KafkaTopicManager()

        mock_admin_client.assert_called_once_with(bootstrap_servers="kafka1:19092")

    def test_init_with_custom_bootstrap_servers(self, mock_admin_client, monkeypatch):
        """Test KafkaTopicManager uses explicitly supplied bootstrap servers."""
        monkeypatch.setattr(
            topic_manager_module,
            "settings",
            SimpleNamespace(kafka_bootstrap_servers="broker-from-settings:9092"),
        )
        custom_servers = "kafka1:9092,kafka2:9092"
        KafkaTopicManager(bootstrap_servers=custom_servers)
        mock_admin_client.assert_called_once_with(bootstrap_servers=custom_servers)

    def test_create_topic_success(self, kafka_manager):
        """Test successful topic creation."""
        kafka_manager.create_topic("test_topic", num_partitions=3, replication_factor=1)
        
        kafka_manager.admin_client.create_topics.assert_called_once()
        call_args = kafka_manager.admin_client.create_topics.call_args
        new_topics = call_args[1]['new_topics']
        
        assert len(new_topics) == 1
        assert new_topics[0].name == "test_topic"
        assert new_topics[0].num_partitions == 3
        assert new_topics[0].replication_factor == 1

    def test_create_topic_with_default_parameters(self, kafka_manager):
        """Test topic creation with default partition and replication parameters."""
        kafka_manager.create_topic("default_topic")
        
        kafka_manager.admin_client.create_topics.assert_called_once()
        call_args = kafka_manager.admin_client.create_topics.call_args
        new_topics = call_args[1]['new_topics']
        
        assert new_topics[0].num_partitions == 1
        assert new_topics[0].replication_factor == 1

    def test_create_topic_already_exists(self, kafka_manager):
        """Test handling when topic already exists."""
        kafka_manager.admin_client.create_topics.side_effect = TopicAlreadyExistsError()
        
        # Should not raise exception
        kafka_manager.create_topic("existing_topic")
        kafka_manager.admin_client.create_topics.assert_called_once()

    def test_create_topic_generic_exception(self, kafka_manager):
        """Test handling of generic exceptions during topic creation."""
        kafka_manager.admin_client.create_topics.side_effect = Exception("Connection error")
        
        # Should not raise exception
        kafka_manager.create_topic("test_topic")
        kafka_manager.admin_client.create_topics.assert_called_once()

    def test_delete_topic_success(self, kafka_manager):
        """Test successful topic deletion."""
        kafka_manager.delete_topic("test_topic")
        
        kafka_manager.admin_client.delete_topics.assert_called_once_with(topics=["test_topic"])

    def test_delete_topic_exception(self, kafka_manager):
        """Test exception handling during topic deletion."""
        kafka_manager.admin_client.delete_topics.side_effect = Exception("Delete failed")
        
        # Should not raise exception
        kafka_manager.delete_topic("test_topic")
        kafka_manager.admin_client.delete_topics.assert_called_once()

    def test_list_topics_success(self, kafka_manager):
        """Test successful topic listing."""
        expected_topics = {'topic1': None, 'topic2': None}
        kafka_manager.admin_client.list_topics.return_value = expected_topics
        
        result = kafka_manager.list_topics()
        
        assert result == expected_topics
        kafka_manager.admin_client.list_topics.assert_called_once()

    def test_list_topics_exception_returns_empty_list(self, kafka_manager):
        """Test that list_topics returns empty list on exception."""
        kafka_manager.admin_client.list_topics.side_effect = Exception("Connection error")
        
        result = kafka_manager.list_topics()
        
        assert result == []
        kafka_manager.admin_client.list_topics.assert_called_once()

    def test_init_topics_success(self, kafka_manager, topic_configs):
        """Test successful initialization of all configured topics."""
        kafka_manager.init_topics()
        
        kafka_manager.admin_client.create_topics.assert_called_once()
        call_args = kafka_manager.admin_client.create_topics.call_args
        new_topics = call_args[1]['new_topics']
        
        assert len(new_topics) == len(topic_configs)
        
        # Verify all topics are created with correct settings
        created_names = {t.name for t in new_topics}
        expected_names = {topic.value for topic in F1Topic}
        assert created_names == expected_names

    def test_init_topics_some_already_exist(self, kafka_manager):
        """Test init_topics when some topics already exist."""
        kafka_manager.admin_client.create_topics.side_effect = TopicAlreadyExistsError("Some topics exist")
        
        # Should not raise exception
        kafka_manager.init_topics()
        kafka_manager.admin_client.create_topics.assert_called_once()

    def test_init_topics_general_exception(self, kafka_manager):
        """Test init_topics exception handling."""
        kafka_manager.admin_client.create_topics.side_effect = Exception("Creation failed")
        
        # Should not raise exception
        kafka_manager.init_topics()
        kafka_manager.admin_client.create_topics.assert_called_once()

    def test_close_success(self, kafka_manager):
        """Test successful close of admin client."""
        kafka_manager.close()
        
        kafka_manager.admin_client.close.assert_called_once()

    def test_context_manager_enter(self, kafka_manager):
        """Test context manager __enter__ method."""
        result = kafka_manager.__enter__()
        assert result is kafka_manager

    def test_context_manager_exit(self, kafka_manager):
        """Test context manager __exit__ method."""
        kafka_manager.__exit__(None, None, None)
        kafka_manager.admin_client.close.assert_called_once()

    def test_context_manager_usage(self, mock_admin_client):
        """Test using KafkaTopicManager as context manager."""
        mock_instance = MagicMock()
        mock_admin_client.return_value = mock_instance
        
        with KafkaTopicManager(bootstrap_servers="localhost:9092") as manager:
            assert isinstance(manager, KafkaTopicManager)
        
        mock_instance.close.assert_called_once()

    def test_topic_configs_has_all_f1_topics(self, topic_configs):
        """Test that the streaming configuration includes all F1 topics."""
        configured_topics = set(topic_configs)
        all_f1_topics = {topic.value for topic in F1Topic}
        
        assert configured_topics == all_f1_topics

    def test_topic_configs_partition_values(self, topic_configs):
        """Test that topic configurations have valid partition values."""
        for config in topic_configs.values():
            assert 'num_partitions' in config
            assert 'replication_factor' in config
            assert config['num_partitions'] > 0
            assert config['replication_factor'] > 0

    def test_create_topic_calls_with_validate_only_false(self, kafka_manager):
        """Test that create_topics is called with validate_only=False."""
        kafka_manager.create_topic("test")
        
        kafka_manager.admin_client.create_topics.assert_called_once()
        call_kwargs = kafka_manager.admin_client.create_topics.call_args[1]
        assert call_kwargs['validate_only'] is False

    def test_init_topics_creates_all_configured_topics_correctly(
        self, kafka_manager, topic_configs
    ):
        """Test that init_topics uses the streaming configuration."""
        kafka_manager.init_topics()
        
        call_args = kafka_manager.admin_client.create_topics.call_args
        new_topics = call_args[1]['new_topics']
        
        # Verify each topic matches its configuration
        for new_topic in new_topics:
            config = topic_configs[new_topic.name]
            assert new_topic.num_partitions == config['num_partitions']
            assert new_topic.replication_factor == config['replication_factor']
