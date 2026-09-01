import pytest
from abc import ABC
from streaming.base import BaseTopicManager


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
