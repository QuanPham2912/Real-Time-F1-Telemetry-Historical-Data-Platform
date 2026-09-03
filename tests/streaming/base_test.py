import pytest
from abc import ABC
from typing import Callable, Iterable, Optional
from metadata import f1_topic
from streaming.base import BaseTopicManager, baseProducer


class TestBaseTopicManager:
    """Test suite for BaseTopicManager abstract class."""

    def test_base_topic_manager_is_abstract(self):
        """Test that BaseTopicManager cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseTopicManager()

    def test_base_topic_manager_is_abc(self):
        """Test that BaseTopicManager is an Abstract Base Class."""
        assert issubclass(BaseTopicManager, ABC)

    def test_base_topic_manager_has_abstract_methods(self):
        """Test that BaseTopicManager has required abstract methods."""
        abstract_methods = BaseTopicManager.__abstractmethods__
        expected_methods = {'create_topic', 'delete_topic', 'list_topics', 'init_topics', 'close'}
        assert abstract_methods == expected_methods

    def test_concrete_implementation_requires_all_methods(self):
        """Test that concrete implementation must implement all abstract methods."""
        
        class IncompleteTopicManager(BaseTopicManager):
            """Incomplete implementation missing some methods."""
            def create_topic(self, topic_name: str, num_partitions: int, replication_factor: int):
                pass

        # Should raise TypeError because not all abstract methods are implemented
        with pytest.raises(TypeError):
            IncompleteTopicManager()

    def test_complete_concrete_implementation_succeeds(self):
        """Test that complete concrete implementation can be instantiated."""
        
        class CompleteTopicManager(BaseTopicManager):
            """Complete implementation of all abstract methods."""
            def create_topic(self, topic_name: str, num_partitions: int, replication_factor: int):
                pass
            
            def delete_topic(self, topic_name: str):
                pass
            
            def list_topics(self) -> list:
                return []
            
            def init_topics(self):
                pass
            
            def close(self):
                pass

        # Should succeed
        manager = CompleteTopicManager()
        assert isinstance(manager, BaseTopicManager)
        assert isinstance(manager, ABC)


class TestBaseProducer:
    """Test suite for baseProducer abstract class."""

    def test_base_producer_is_abstract(self):
        """Test that baseProducer cannot be instantiated directly."""
        with pytest.raises(TypeError):
            baseProducer()

    def test_base_producer_is_abc(self):
        """Test that baseProducer is an Abstract Base Class."""
        assert issubclass(baseProducer, ABC)

    def test_base_producer_has_abstract_methods(self):
        """Test that baseProducer has all required abstract methods."""
        abstract_methods = baseProducer.__abstractmethods__
        expected_methods = {'send', 'send_many', 'close', 'flush'}
        assert abstract_methods == expected_methods

    def test_incomplete_producer_cannot_be_instantiated(self):
        """Test that a producer must implement all abstract methods."""

        class IncompleteProducer(baseProducer):
            def send(self, topic_name: str, message: dict, key: str = None):
                pass

        with pytest.raises(TypeError):
            IncompleteProducer()

    def test_complete_producer_can_be_instantiated(self):
        """Test that a complete producer implementation can be instantiated."""

        class CompleteProducer(baseProducer):
            def send(self, topic_name: f1_topic.F1Topic, message: dict, key: str = None):
                pass

            def send_many(self, topic_name: f1_topic.F1Topic, messages: Iterable[dict] = None, key_builder: Optional[Callable[[dict], str]] = None):
                pass

            def close(self):
                pass

            def flush(self):
                pass

        producer = CompleteProducer()
        assert isinstance(producer, baseProducer)
        assert isinstance(producer, ABC)
